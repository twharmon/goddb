package goddb

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

var ErrItemNotFound = errors.New("item not found")

type GetRequest[T any] struct {
	value *T
	input *dynamodb.GetItemInput
}

func (r *GetRequest[T]) Exec() (*T, error) {
	wrap := func(err error) error {
		return fmt.Errorf("goddb get: %w", err)
	}
	val, err := valueOf(r.value)
	if err != nil {
		return r.value, wrap(err)
	}
	r.input.Key, err = makeItem(val.Type(), val, func(attr string) bool { return attr == "SK" || attr == "PK" })
	if err != nil {
		return r.value, wrap(err)
	}
	output, err := client.GetItem(context.Background(), r.input)
	if err != nil {
		return r.value, wrap(err)
	}
	if len(output.Item) == 0 {
		return r.value, ErrItemNotFound
	}
	if err := setFieldValues(val, output.Item); err != nil {
		return nil, wrap(err)
	}
	return r.value, nil
}

func (r *GetRequest[T]) Consistent() *GetRequest[T] {
	r.input.ConsistentRead = aws.Bool(true)
	return r
}

func Get[T any](v *T) *GetRequest[T] {
	return &GetRequest[T]{
		value: v,
		input: &dynamodb.GetItemInput{
			TableName: aws.String(os.Getenv("GODDB_TABLE_NAME")),
		},
	}
}
