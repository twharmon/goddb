package goddb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type PutRequest[T any] struct {
	input     *dynamodb.PutItemInput
	item      *T
	condition *Condition[T]
}

func Put[T any](item *T) *PutRequest[T] {
	return &PutRequest[T]{
		item: item,
		input: &dynamodb.PutItemInput{
			TableName: aws.String(os.Getenv("GODDB_TABLE_NAME")),
		},
	}
}

type tagValuePair struct {
	tag   string
	value reflect.Value
}

func (r *PutRequest[T]) If(condition *Condition[T]) *PutRequest[T] {
	r.condition = condition
	return r
}

func (r *PutRequest[T]) Exec() error {
	wrap := func(err error) error {
		return fmt.Errorf("goddb put: %w", err)
	}
	r.input.Item = make(map[string]types.AttributeValue)
	val, err := valueOf(r.item)
	if err != nil {
		return wrap(err)
	}
	ty := val.Type()
	r.input.Item, err = makeItem(ty, val, func(attr string) bool { return true })
	if err != nil {
		return wrap(err)
	}
	skAttrCounts := make(map[string]int)
	for i := 0; i < ty.NumField(); i++ {
		ft := ty.Field(i)
		if !ft.IsExported() {
			continue
		}
		if tag := ft.Tag.Get("goddb"); tag != "" {
			attrs := strings.Split(tag, ",")
			for _, attr := range attrs {
				if strings.HasSuffix(attr, "SK") {
					skAttrCounts[attr]++
					if skAttrCounts[attr] > 1 {
						return wrap(fmt.Errorf("found more than one field with sort key %s", attr))
					}
				}
			}
			if val.Field(i).IsZero() {
				return wrap(fmt.Errorf("field %s can not be zero value", ft.Name))
			}
		}
	}
	if r.condition != nil {
		exp, names, values, err := r.condition.expression(len(r.input.ExpressionAttributeValues))
		if err != nil {
			return wrap(err)
		}
		r.input.ConditionExpression = &exp
		r.input.ExpressionAttributeNames = merge(r.input.ExpressionAttributeNames, names)
		r.input.ExpressionAttributeValues = merge(r.input.ExpressionAttributeValues, values)
	}
	_, err = client.PutItem(context.Background(), r.input)
	if err != nil {
		var ex *types.ConditionalCheckFailedException
		if errors.As(err, &ex) {
			return ErrConditionFailed
		}
		return wrap(err)
	}
	return nil
}
