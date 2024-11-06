package goddb

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type DeleteRequest[T any] struct {
	value *T
	input *dynamodb.DeleteItemInput
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
	_, err = client.DeleteItem(context.Background(), r.input)
	if err != nil {
		return wrap(err)
	}
	return nil
}

func Delete[T any](v *T) *DeleteRequest[T] {
	return &DeleteRequest[T]{
		value: v,
		input: &dynamodb.DeleteItemInput{
			TableName: aws.String(os.Getenv("GODDB_TABLE_NAME")),
		},
	}
}
