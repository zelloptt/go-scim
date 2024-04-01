package crud

import (
	"fmt"

	"github.com/imulab/go-scim/pkg/v2/annotation"
	"github.com/imulab/go-scim/pkg/v2/crud/expr"
	"github.com/imulab/go-scim/pkg/v2/prop"
	"github.com/imulab/go-scim/pkg/v2/spec"
)

type traverseCb func(nav prop.Navigator) error
type traverseUpdatedValueCb func(nav prop.Navigator, value interface{}) error

func defaultTraverse(property prop.Property, query *expr.Expression, callback traverseCb) error {
	tr := traverser{
		nav:             prop.Navigate(property),
		callback:        callback,
		elementStrategy: selectAllStrategy,
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
//
// It returns error if there is already a sub property by this filter
func addByEqFilterTraverse(value interface{}, property prop.Property, query *expr.Expression, callback traverseUpdatedValueCb) error {
	return traverser{
		valueByEqFilter:      value,
		addByEqFilter:        true,
		nav:                  prop.Navigate(property),
		callbackUpdatedValue: callback,
		elementStrategy:      selectAllStrategy,
	}.traverse(query)
}

func primaryOrFirstTraverse(property prop.Property, query *expr.Expression, callback traverseCb) error {
	return traverser{
		nav:             prop.Navigate(property),
		callback:        callback,
		elementStrategy: primaryOrFirstStrategy,
	}.traverse(query)
}

type traverser struct {
	addByEqFilter        bool                   // true if valueByEqFilter should be updated by the Eq filter
	valueByEqFilter      interface{}            // value to be updated using the Eq filter
	nav                  prop.Navigator         // stateful navigator for the resource being traversed
	callback             traverseCb             // callback function to be invoked when target is reached
	callbackUpdatedValue traverseUpdatedValueCb // callback function to be invoked with an updated value when target is reached
	elementStrategy      elementStrategy        // strategy to select element properties to traverse for multiValued properties
}

func (t traverser) traverse(query *expr.Expression) error {
	if query == nil {
		return t.callback(t.nav)
	}

	if query.IsRootOfFilter() {
		if !t.nav.Current().Attribute().MultiValued() {
			return fmt.Errorf("%w: filter applied to singular attribute", spec.ErrInvalidFilter)
		}
		if t.addByEqFilter && query.Token() == expr.Eq {
			return t.updateValueByEqFilter(query)
		}
		return t.traverseQualifiedElements(query)
	}

	if t.nav.Current().Attribute().MultiValued() {
		return t.traverseSelectedElements(query)
	}

	return t.traverseNext(query)
}

func (t traverser) updateValueByEqFilter(query *expr.Expression) error {
	var err error
	var filterValue interface{}
	value := t.valueByEqFilter
	keyValue := ""
	filterKey := ""

	if query.Left() != nil && query.Left().IsPath() {
		filterKey = query.Left().Token()
	}
	if query.Next() != nil && query.Next().IsPath() {
		if query.Next().Next() != nil {
			return fmt.Errorf("%w: only a single Eq filter is applicable", spec.ErrInvalidFilter)
		}
		keyValue = query.Next().Token()
	}
	if filterKey == "" || keyValue == "" {
		return fmt.Errorf("%w: filter is not supported", spec.ErrInvalidFilter)
	}
	if query.Right() != nil && query.Right().IsLiteral() {
		// add a child to the copy of the target property to parse allowed type of filterValue
		propCopy := t.nav.Current().Clone()
		navCopy := prop.Navigate(propCopy)
		navCopy.Add(map[string]interface{}{})
		navCopy.At(0).Dot(filterKey)
		if navCopy.HasError() {
			// the child does not have a sub property by filterKey
			return fmt.Errorf("%w: invalid filter: %w", spec.ErrInvalidFilter, t.nav.Error())
		}
		filterValue, err = evaluator{}.normalize(
			navCopy.Current().Attribute(),
			query.Right().Token(),
		)
		if err != nil {
			return fmt.Errorf("%w: invalid filter value: %w", spec.ErrInvalidFilter, err)
		}
	}
	if t.callbackUpdatedValue == nil {
		return fmt.Errorf("%w: callbackUpdatedValue not initiated", spec.ErrInternal)
	}
	return t.callbackUpdatedValue(t.nav, []interface{}{
		map[string]interface{}{
			keyValue:  value,
			filterKey: filterValue,
		}})
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
