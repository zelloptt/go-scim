package crud

import (
	"fmt"

	"github.com/imulab/go-scim/pkg/v2/annotation"
	"github.com/imulab/go-scim/pkg/v2/crud/expr"
	"github.com/imulab/go-scim/pkg/v2/prop"
	"github.com/imulab/go-scim/pkg/v2/spec"
)

type traverseCb func(nav prop.Navigator) error
type traverseAddAttributeCb func(nav prop.Navigator, value interface{}) error

func defaultTraverse(property prop.Property, query *expr.Expression, callback traverseCb) error {
	tr := traverser{
		nav:             prop.Navigate(property),
		callback:        callback,
		elementStrategy: selectAllStrategy,
	}
	return tr.traverse(query)
}

func addByEqualOperatorTraverse(value interface{}, property prop.Property, query *expr.Expression, callback traverseAddAttributeCb) error {
	// A single 'Eq' filter can be used to add a new attribute
	return traverser{
		value:                value,
		addByEqual:           true,
		nav:                  prop.Navigate(property),
		callbackAddAttribute: callback,
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
	addByEqual           bool
	value                interface{}            // value to be updated using the Eq fiter
	nav                  prop.Navigator         // stateful navigator for the resource being traversed
	callback             traverseCb             // callback function to be invoked when target is reached
	callbackAddAttribute traverseAddAttributeCb // callback function to be invoked when target is reached
	elementStrategy      elementStrategy        // strategy to select element properties to traverse for multiValued properties
}

func (t traverser) invokeCallback(nav prop.Navigator, value interface{}) error {
	if t.callbackAddAttribute != nil {
		if err := t.callbackAddAttribute(nav, value); err != nil {
			return err
		}
	}
	if t.callback != nil {
		if err := t.callback(nav); err != nil {
			return err
		}
	}
	return nil
}

func (t traverser) traverse(query *expr.Expression) error {
	if query == nil {
		return t.invokeCallback(t.nav, nil)
	}

	if query.IsRootOfFilter() {
		if !t.nav.Current().Attribute().MultiValued() {
			return fmt.Errorf("%w: filter applied to singular attribute", spec.ErrInvalidFilter)
		}
		err, isFound := t.traverseQualifiedElements(query)
		if err != nil {
			return err
		}
		if !isFound && t.addByEqual && query.Token() == expr.Eq {
			value := t.value
			keyValue := ""
			filterKey := ""
			filterValue := ""
			if query.Next() != nil {
				if query.Next().Next() != nil {
					return fmt.Errorf("%w: only a single Eq filter is applicable", spec.ErrInvalidFilter)
				}
				keyValue = query.Next().Token()
			}
			if query.Left() != nil {
				filterKey = query.Left().Token()
			}
			if query.Right() != nil {
				filterValue = query.Right().Token()
			}
			return t.invokeCallback(t.nav, []interface{}{
				map[string]interface{}{
					keyValue:  value,
					filterKey: filterValue,
				}})
		}
	}

	if t.nav.Current().Attribute().MultiValued() {
		return t.traverseSelectedElements(query)
	}

	return t.traverseNext(query)
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

func (t traverser) traverseQualifiedElements(filter *expr.Expression) (error, bool) {
	isFound := false
	err := t.nav.ForEachChild(func(index int, child prop.Property) error {
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

		isFound = true
		return t.traverse(filter.Next())
	})
	return err, isFound
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
