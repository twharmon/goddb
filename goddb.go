package goddb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

var cfg = must(config.LoadDefaultConfig(context.Background()))
var client = dynamodb.NewFromConfig(cfg)
var TagChar = '#'

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
