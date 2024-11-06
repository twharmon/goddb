package goddb

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type UpdateRequest[T any] struct {
	item    *T
	sets    []*T
	adds    []*T
	deletes []*T
	removes []func(*T) any
	input   *dynamodb.UpdateItemInput
}

func Update[T any](item *T) *UpdateRequest[T] {
	return &UpdateRequest[T]{
		input: &dynamodb.UpdateItemInput{
			TableName: aws.String(os.Getenv("GODDB_TABLE_NAME")),
		},
		item: item,
	}
}

func (r *UpdateRequest[T]) Set(t *T) *UpdateRequest[T] {
	r.sets = append(r.sets, t)
	return r
}

func (r *UpdateRequest[T]) Add(t *T) *UpdateRequest[T] {
	r.adds = append(r.adds, t)
	return r
}

func (r *UpdateRequest[T]) Remove(remove func(t *T) any) *UpdateRequest[T] {
	r.removes = append(r.removes, remove)
	return r
}

func (r *UpdateRequest[T]) Delete(t *T) *UpdateRequest[T] {
	r.deletes = append(r.deletes, t)
	return r
}

func (r *UpdateRequest[T]) Exec() error {
	wrap := func(err error) error {
		return fmt.Errorf("goddb update: %w", err)
	}
	val, err := valueOf(r.item)
	if err != nil {
		return wrap(err)
	}
	r.input.Key, err = makeItem(val.Type(), val, func(attr string) bool { return attr == "SK" || attr == "PK" })
	var exp strings.Builder
	if err := r.updateExpressionSet(&exp); err != nil {
		return wrap(err)
	}
	if err := r.updateExpressionAdd(&exp); err != nil {
		return wrap(err)
	}
	if err := r.updateExpressionDelete(&exp); err != nil {
		return wrap(err)
	}
	if err := r.updateExpressionRemove(&exp); err != nil {
		return wrap(err)
	}
	r.input.UpdateExpression = aws.String(exp.String())
	_, err = client.UpdateItem(context.Background(), r.input)
	if err != nil {
		return wrap(err)
	}
	return nil
}

func (r *UpdateRequest[T]) updateExpressionSet(exp *strings.Builder) error {
	if len(r.sets) > 0 {
		if exp.Len() > 0 {
			exp.WriteRune(' ')
		}
		exp.WriteString("SET ")
	}
	var hit bool
	for _, set := range r.sets {
		v, err := valueOf(set)
		if err != nil {
			return err
		}
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			fv := v.Field(i)
			ft := t.Field(i)
			if !ft.IsExported() {
				continue
			}
			if fv.IsZero() {
				continue
			}
			expAttrVal, err := r.getExpressionAttributeValue(fv)
			if err != nil {
				return err
			}
			if expAttrVal == "" {
				continue
			}
			attrName := r.getExpressionAttributeName(ft.Name)
			if hit {
				exp.WriteString(", ")
			}
			hit = true
			exp.WriteString(attrName)
			exp.WriteString(" = ")
			exp.WriteString(expAttrVal)
		}
	}
	return nil
}

func (r *UpdateRequest[T]) getExpressionAttributeName(name string) string {
	if r.input.ExpressionAttributeNames == nil {
		r.input.ExpressionAttributeNames = make(map[string]string)
	}
	attrName := fmt.Sprintf("#%s", name)
	r.input.ExpressionAttributeNames[attrName] = name
	return attrName
}

// getExpressionAttributeValue can return "", nil
func (r *UpdateRequest[T]) getExpressionAttributeValue(value reflect.Value) (string, error) {
	av, err := makeAttributeValue(value)
	if err != nil {
		return "", err
	}
	if av == nil {
		return "", nil
	}
	if r.input.ExpressionAttributeValues == nil {
		r.input.ExpressionAttributeValues = make(map[string]types.AttributeValue)
	}
	attrValue := fmt.Sprintf(":%d", len(r.input.ExpressionAttributeValues))
	r.input.ExpressionAttributeValues[attrValue] = av
	return attrValue, nil
}

func (r *UpdateRequest[T]) updateExpressionAdd(exp *strings.Builder) error {
	if len(r.adds) > 0 {
		if exp.Len() > 0 {
			exp.WriteRune(' ')
		}
		exp.WriteString("ADD ")
	}
	var hit bool
	for _, add := range r.adds {
		v, err := valueOf(add)
		if err != nil {
			return err
		}
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			fv := v.Field(i)
			ft := t.Field(i)
			if !ft.IsExported() {
				continue
			}
			if fv.IsZero() {
				continue
			}
			expAttrVal, err := r.getExpressionAttributeValue(fv)
			if err != nil {
				return err
			}
			if expAttrVal == "" {
				continue
			}
			if hit {
				exp.WriteString(", ")
			}
			hit = true
			attrName := r.getExpressionAttributeName(ft.Name)
			exp.WriteString(attrName)
			exp.WriteString(" ")
			exp.WriteString(expAttrVal)
		}
	}
	return nil
}

func (r *UpdateRequest[T]) updateExpressionDelete(exp *strings.Builder) error {
	if len(r.deletes) > 0 {
		if exp.Len() > 0 {
			exp.WriteRune(' ')
		}
		exp.WriteString("DELETE ")
	}
	var hit bool
	for _, delete := range r.deletes {
		v, err := valueOf(delete)
		if err != nil {
			return err
		}
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			fv := v.Field(i)
			ft := t.Field(i)
			if !ft.IsExported() {
				continue
			}
			if fv.IsZero() {
				continue
			}
			expAttrVal, err := r.getExpressionAttributeValue(fv)
			if err != nil {
				return err
			}
			if expAttrVal == "" {
				continue
			}
			if hit {
				exp.WriteString(", ")
			}
			hit = true
			attrName := r.getExpressionAttributeName(ft.Name)
			exp.WriteString(attrName)
			exp.WriteString(" ")
			exp.WriteString(expAttrVal)
		}
	}
	return nil
}

func (r *UpdateRequest[T]) updateExpressionRemove(exp *strings.Builder) error {
	if len(r.removes) > 0 {
		if exp.Len() > 0 {
			exp.WriteRune(' ')
		}
		exp.WriteString("REMOVE ")
	}
	for i, remove := range r.removes {
		if i > 0 {
			exp.WriteString(", ")
		}
		fieldName := getFieldNameFromTest(remove)
		attrName := r.getExpressionAttributeName(fieldName)
		exp.WriteString(attrName)
	}
	return nil
}
