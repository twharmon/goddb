package goddb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type operator int

const (
	operatorEqual operator = iota
	operatorNotEqual
	operatorAttributeExists
	operatorAttributeNotExists
)

var ErrConditionFailed = errors.New("condition failed")

type Condition[T any] struct {
	and      []*Condition[T]
	or       []*Condition[T]
	value    *T
	operator operator
	selector func(*T) any
	// between:
	// start *T
	// end   *T
}

func And[T any](conditions ...*Condition[T]) *Condition[T] {
	return &Condition[T]{
		and: conditions,
	}
}

func Or[T any](conditions ...*Condition[T]) *Condition[T] {
	return &Condition[T]{
		or: conditions,
	}
}

func Equal[T any](v *T) *Condition[T] {
	return &Condition[T]{
		value:    v,
		operator: operatorEqual,
	}
}

func NotEqual[T any](v *T) *Condition[T] {
	return &Condition[T]{
		value:    v,
		operator: operatorNotEqual,
	}
}

func AttributeExists[T any](selector func(*T) any) *Condition[T] {
	return &Condition[T]{
		selector: selector,
		operator: operatorAttributeExists,
	}
}

func AttributeNotExists[T any](selector func(*T) any) *Condition[T] {
	return &Condition[T]{
		selector: selector,
		operator: operatorAttributeNotExists,
	}
}

func (c *Condition[T]) expression(valCnt int) (string, map[string]string, map[string]types.AttributeValue, error) {
	var exp strings.Builder
	exp.WriteRune('(')
	names := make(map[string]string)
	values := make(map[string]types.AttributeValue)
	if len(c.and) > 0 {
		for _, cond := range c.and {
			str, ns, vs, err := cond.expression(valCnt)
			if err != nil {
				return "", nil, nil, err
			}
			valCnt += len(vs)
			names = merge(names, ns)
			values = merge(values, vs)
			if exp.Len() > 1 {
				exp.WriteString(" and ")
			}
			exp.WriteString(str)
		}
		exp.WriteRune(')')
		return exp.String(), names, values, nil
	}
	if len(c.or) > 0 {
		for _, cond := range c.or {
			str, ns, vs, err := cond.expression(valCnt)
			if err != nil {
				return "", nil, nil, err
			}
			valCnt += len(vs)
			names = merge(names, ns)
			values = merge(values, vs)
			if exp.Len() > 1 {
				exp.WriteString(" or ")
			}
			exp.WriteString(str)
		}
		exp.WriteRune(')')
		return exp.String(), names, values, nil
	}
	switch c.operator {
	case operatorAttributeExists:
		fieldName := getFieldNameFromTest(c.selector)
		name := fmt.Sprintf("#%s", fieldName)
		exp := fmt.Sprintf("attribute_exists(%s)", name)
		return exp, map[string]string{name: fieldName}, nil, nil
	case operatorAttributeNotExists:
		fieldName := getFieldNameFromTest(c.selector)
		name := fmt.Sprintf("#%s", fieldName)
		exp := fmt.Sprintf("attribute_not_exists(%s)", name)
		return exp, map[string]string{name: fieldName}, nil, nil
	case operatorEqual:
		nameVals, err := c.getNameValues()
		if err != nil {
			return "", nil, nil, err
		}
		for k, v := range nameVals {
			name := fmt.Sprintf("#%s", k)
			value := fmt.Sprintf(":%d", valCnt)
			valCnt++
			if exp.Len() > 1 {
				exp.WriteString(" and ")
			}
			exp.WriteString(name)
			exp.WriteString(" = ")
			exp.WriteString(value)
			names[name] = k
			values[value] = v
		}
	case operatorNotEqual:
		nameVals, err := c.getNameValues()
		if err != nil {
			return "", nil, nil, err
		}
		for k, v := range nameVals {
			name := fmt.Sprintf("#%s", k)
			value := fmt.Sprintf(":%d", valCnt)
			valCnt++
			if exp.Len() > 1 {
				exp.WriteString(" and ")
			}
			exp.WriteString(name)
			exp.WriteString(" <> ")
			exp.WriteString(value)
			names[name] = k
			values[value] = v
		}
	}
	exp.WriteRune(')')
	return exp.String(), names, values, nil
}

func (c *Condition[T]) getNameValues() (map[string]types.AttributeValue, error) {
	val, err := valueOf(c.value)
	if err != nil {
		return nil, err
	}
	m := make(map[string]types.AttributeValue)
	ty := val.Type()
	for i := 0; i < ty.NumField(); i++ {
		sf := ty.Field(i)
		if !sf.IsExported() {
			continue
		}
		fv := val.Field(i)
		if fv.IsZero() {
			continue
		}
		m[sf.Name], err = makeAttributeValue(fv)
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}
