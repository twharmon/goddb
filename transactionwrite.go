package goddb

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type TransactionWriteRequest struct {
	puts    []any
	deletes []any
}

func TransactionWrite() *TransactionWriteRequest {
	return &TransactionWriteRequest{}
}

func (t *TransactionWriteRequest) Put(value any) *TransactionWriteRequest {
	t.puts = append(t.puts, value)
	return t
}

func (t *TransactionWriteRequest) Delete(value any) *TransactionWriteRequest {
	t.deletes = append(t.deletes, value)
	return t
}

func (t *TransactionWriteRequest) Exec() error {
	wrap := func(err error) error {
		return fmt.Errorf("goddb transaction write items: %w", err)
	}
	var items []types.TransactWriteItem
	for _, put := range t.puts {
		val, err := valueOf(put)
		if err != nil {
			return wrap(err)
		}
		ty := val.Type()
		item, err := makeItem(ty, val, func(attr string) bool { return true })
		if err != nil {
			return wrap(err)
		}
		if err := validateCompleteKey(ty, val); err != nil {
			return wrap(err)
		}
		items = append(items, types.TransactWriteItem{
			Put: &types.Put{Item: item, TableName: aws.String(os.Getenv("GODDB_TABLE_NAME"))},
		})
	}
	for _, del := range t.deletes {
		val, err := valueOf(del)
		if err != nil {
			return wrap(err)
		}
		ty := val.Type()
		item, err := makeItem(ty, val, func(attr string) bool { return attr == "SK" || attr == "PK" })
		if err != nil {
			return wrap(err)
		}
		if err := validateCompleteKey(ty, val); err != nil {
			return wrap(err)
		}
		items = append(items, types.TransactWriteItem{
			Delete: &types.Delete{Key: item, TableName: aws.String(os.Getenv("GODDB_TABLE_NAME"))},
		})
	}
	if _, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	}); err != nil {
		return wrap(err)
	}
	return nil
}
