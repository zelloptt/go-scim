package crud

import (
	"fmt"

	"github.com/imulab/go-scim/pkg/v2/annotation"
	"github.com/imulab/go-scim/pkg/v2/crud/expr"
	"github.com/imulab/go-scim/pkg/v2/prop"
	"github.com/imulab/go-scim/pkg/v2/spec"
)

type traverseCb func(nav prop.Navigator) error

func defaultTraverse(property prop.Property, query *expr.Expression, callback traverseCb) error {
	cb := func(nav prop.Navigator, query *expr.Expression) error {
		return callback(nav)
	}
	tr := traverser{
		nav:              prop.Navigate(property),
		callback:         cb,
		elementStrategy:  selectAllStrategy,
		traverseStrategy: traverseAll,
	}
	return tr.traverse(query)
}

// A single 'Eq' filter can be used to add a new attribute.
// This traverse calls the callback with the modified value using such filter.
// The operation like:
//
//	{
//		"op": "add",
//		"path": "emails[type eq \"work\"].value",
//		"value": "foo@bar.com"
//	}
//
// Adds a new property:
//
//	"emails": [
//		{
//			"type": "work",
//			"value": "foo@bar.com"
//		}
//	]
func addByEqFilterTraverse(value interface{}, property prop.Property, query *expr.Expression, callback traverseCb) error {
	cb := func(nav prop.Navigator, query *expr.Expression) error {
		v, err := composeValueByEqFilter(value, query, nav)
		if err != nil {
			return err
		}
		nav.Add(v)
		return callback(nav)
	}
	return traverser{
		nav:              prop.Navigate(property),
		callback:         cb,
		elementStrategy:  selectAllStrategy,
		traverseStrategy: traverseToSingleEqFilter,
	}.traverse(query)
}

func primaryOrFirstTraverse(property prop.Property, query *expr.Expression, callback traverseCb) error {
	cb := func(nav prop.Navigator, query *expr.Expression) error {
		return callback(nav)
	}
	return traverser{
		nav:              prop.Navigate(property),
		callback:         cb,
		elementStrategy:  primaryOrFirstStrategy,
		traverseStrategy: traverseAll,
	}.traverse(query)
}

type traverser struct {
	nav              prop.Navigator                                         // stateful navigator for the resource being traversed
	elementStrategy  elementStrategy                                        // strategy to select element properties to traverse for multiValued properties
	traverseStrategy traverseStrategy                                       // strategy to stop traversing the query
	callback         func(nav prop.Navigator, query *expr.Expression) error // callback to be invoked when target is reached
}

func (t traverser) traverse(query *expr.Expression) error {
	traverseDone := t.traverseStrategy()
	if traverseDone(t.nav, query) {
		return t.callback(t.nav, query)
	}

	if query.IsRootOfFilter() {
		if !t.nav.Current().Attribute().MultiValued() {
			return fmt.Errorf("%w: filter applied to singular attribute", spec.ErrInvalidFilter)
		}
		return t.traverseQualifiedElements(query)
	}

	if t.nav.Current().Attribute().MultiValued() {
		return t.traverseSelectedElements(query)
	}

	return t.traverseNext(query)
}

func composeValueByEqFilter(value interface{}, query *expr.Expression, nav prop.Navigator) (interface{}, error) {
	var err error
	var filterValue interface{}
	keyValue := ""
	filterKey := ""

	if query == nil {
		return nil, fmt.Errorf("%w: no filter found", spec.ErrInvalidFilter)
	}

	if query.Left() != nil && query.Left().IsPath() {
		filterKey = query.Left().Token()
	}
	if query.Next() != nil && query.Next().IsPath() {
		if query.Next().Next() != nil {
			return nil, fmt.Errorf("%w: only a single Eq filter is applicable", spec.ErrInvalidFilter)
		}
		keyValue = query.Next().Token()
	}
	if filterKey == "" || keyValue == "" {
		return nil, fmt.Errorf("%w: filter is not supported", spec.ErrInvalidFilter)
	}
	if query.Right() != nil && query.Right().IsLiteral() {
		// add a child to the copy of the target property to parse allowed type of filterValue
		propCopy := nav.Current().Clone()
		navCopy := prop.Navigate(propCopy)
		navCopy.Add(map[string]interface{}{})
		navCopy.At(0).Dot(filterKey)
		if navCopy.HasError() {
			// the child does not have a sub property by filterKey
			return nil, fmt.Errorf("%w: invalid filter: %w", spec.ErrInvalidFilter, navCopy.Error())
		}
		filterValue, err = evaluator{}.normalize(
			navCopy.Current().Attribute(),
			query.Right().Token(),
		)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid filter value: %w", spec.ErrInvalidFilter, err)
		}
	}
	return []interface{}{
		map[string]interface{}{
			keyValue:  value,
			filterKey: filterValue,
		}}, nil
}

func (t traverser) traverseNext(query *expr.Expression) error {
	t.nav.Dot(query.Token())
	if err := t.nav.Error(); err != nil {
		return err
	}
	defer t.nav.Retract()

	return t.traverse(query.Next())
}

func (t traverser) traverseSelectedElements(query *expr.Expression) error {
	selector := t.elementStrategy(t.nav.Current())

	return t.nav.Current().ForEachChild(func(index int, child prop.Property) error {
		if !selector(index, child) { // skip elements not satisfied by strategy
			return nil
		}

		t.nav.At(index)
		if err := t.nav.Error(); err != nil {
			return err
		}
		defer t.nav.Retract()

		return t.traverse(query)
	})
}

func (t traverser) traverseQualifiedElements(filter *expr.Expression) error {
	return t.nav.ForEachChild(func(index int, child prop.Property) error {
		t.nav.At(index)
		if err := t.nav.Error(); err != nil {
			return err
		}
		defer t.nav.Retract()

		r, err := evaluator{base: t.nav.Current(), filter: filter}.evaluate()
		if err != nil {
			return err
		} else if !r {
			return nil
		}

		return t.traverse(filter.Next())
	})
}

type traverseStrategy func() func(nav prop.Navigator, query *expr.Expression) bool

var (
	// strategy to traverse all query
	traverseAll traverseStrategy = func() func(nav prop.Navigator, query *expr.Expression) bool {
		return func(nav prop.Navigator, query *expr.Expression) bool {
			return query == nil
		}
	}

	// strategy to get the root of the only Eq filter
	traverseToSingleEqFilter traverseStrategy = func() func(nav prop.Navigator, query *expr.Expression) bool {
		return func(nav prop.Navigator, query *expr.Expression) bool {
			if query == nil {
				// If query has been traversed and there is no Eq filter - finish the traverse
				return true
			}
			if !query.IsRootOfFilter() {
				// Looking for the root of an Eq filter
				return false
			}
			if !nav.Current().Attribute().MultiValued() {
				// Filter is not applicable to a singular attribute
				return false
			}
			if query.Token() != expr.Eq {
				// Only an Eq filter is supported
				return false
			}
			if query.Left() == nil || !query.Left().IsPath() {
				// The left expression should reflect an attribute path
				return false
			}
			if query.Next() == nil || !query.Next().IsPath() || query.Next().Next() != nil {
				// Only a single non-complex filter is supported
				return false
			}
			if query.Right() == nil || !query.Right().IsLiteral() {
				// The right expression should be a value assignable to an attribute
				return false
			}
			return true
		}
	}
)

type elementStrategy func(multiValuedComplex prop.Property) func(index int, child prop.Property) bool

var (
	// strategy to traverse all children elements
	selectAllStrategy elementStrategy = func(multiValuedComplex prop.Property) func(index int, child prop.Property) bool {
		return func(index int, child prop.Property) bool {
			return true
		}
	}
	// strategy to traverse the element whose primary attribute is true, or the first element when no primary attribute is true
	primaryOrFirstStrategy elementStrategy = func(multiValuedComplex prop.Property) func(index int, child prop.Property) bool {
		primaryAttr := multiValuedComplex.Attribute().FindSubAttribute(func(subAttr *spec.Attribute) bool {
			_, ok := subAttr.Annotation(annotation.Primary)
			return ok && subAttr.Type() == spec.TypeBoolean
		})

		if primaryAttr != nil {
			truePrimary := multiValuedComplex.FindChild(func(child prop.Property) bool {
				p, err := child.ChildAtIndex(primaryAttr.Name())
				return err == nil && p != nil && p.Raw() == true
			})
			if truePrimary != nil {
				return func(index int, child prop.Property) bool {
					return child == truePrimary
				}
			}
		}

		return func(index int, child prop.Property) bool {
			return index == 0
		}
	}
)
