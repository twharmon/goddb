package goddb

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type DeleteRequest[T any] struct {
	value     *T
	input     *dynamodb.DeleteItemInput
	condition *Condition[T]
}

func (r *DeleteRequest[T]) Exec() error {
	wrap := func(err error) error {
		return fmt.Errorf("goddb delete: %w", err)
	}
	val, err := valueOf(r.value)
	if err != nil {
		return wrap(err)
	}
	r.input.Key, err = makeItem(val.Type(), val, func(attr string) bool { return attr == "SK" || attr == "PK" })
	if err != nil {
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
	}
	_, err = client.DeleteItem(context.Background(), r.input)
	if err != nil {
		return wrap(err)
	}
	return nil
}

func (r *DeleteRequest[T]) If(condition *Condition[T]) *DeleteRequest[T] {
	r.condition = condition
	return r
}

func Delete[T any](v *T) *DeleteRequest[T] {
	return &DeleteRequest[T]{
		value: v,
		input: &dynamodb.DeleteItemInput{
			TableName: aws.String(os.Getenv("GODDB_TABLE_NAME")),
		},
	}
}
