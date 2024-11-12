package goddb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"

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
	val, err := valueOf(r.item)
	if err != nil {
		return wrap(err)
	}
	ty := val.Type()
	r.input.Item, err = makeItem(ty, val, func(attr string) bool { return true })
	if err != nil {
		return wrap(err)
	}
	if err := validateCompleteKey(ty, val); err != nil {
		return wrap(err)
	}
	if r.condition != nil {
		exp, names, values, err := r.condition.expression(len(r.input.ExpressionAttributeValues))
		if err != nil {
			return wrap(err)
		}
		r.input.ConditionExpression = &exp
		r.input.ExpressionAttributeNames = merge(r.input.ExpressionAttributeNames, names)
		r.input.ExpressionAttributeValues = merge(r.input.ExpressionAttributeValues, values)
		fmt.Println(exp)
		fmt.Println(r.input.ExpressionAttributeNames)
		for k, v := range r.input.ExpressionAttributeValues {
			fmt.Println(k, v)
		}
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
