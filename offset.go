package goddb

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var offsetPartSeparator = "."

var offsetKeyValueSeparator = ":"

func offsetToLastEvaluatedKey(offset string) (map[string]types.AttributeValue, error) {
	if offset == "" {
		return nil, nil
	}
	offsetB, err := base64.URLEncoding.DecodeString(offset)
	if err != nil {
		return nil, err
	}
	offset = string(offsetB)
	lek := make(map[string]types.AttributeValue)
	parts := strings.Split(offset, offsetPartSeparator)
	pkParts := strings.Split(parts[0], offsetKeyValueSeparator)
	pkValB, err := base64.URLEncoding.DecodeString(pkParts[1])
	if err != nil {
		return nil, err
	}
	lek[pkParts[0]] = &types.AttributeValueMemberS{Value: string(pkValB)}
	if len(parts) > 1 {
		skParts := strings.Split(parts[1], offsetKeyValueSeparator)
		skValB, err := base64.URLEncoding.DecodeString(skParts[1])
		if err != nil {
			return nil, err
		}
		lek[skParts[0]] = &types.AttributeValueMemberS{Value: string(skValB)}
	}
	return lek, nil
}

func lastEvaluatedKeyToOffset(lek map[string]types.AttributeValue) (string, error) {
	var parts []string
	for k, v := range lek {
		vv, ok := v.(*types.AttributeValueMemberS)
		if !ok {
			return "", errors.New("expected string attributes only in last evaluated key")
		}
		part := fmt.Sprintf("%s%s%s", k, offsetKeyValueSeparator, base64.URLEncoding.EncodeToString([]byte(vv.Value)))
		parts = append(parts, part)
	}
	raw := strings.Join(parts, offsetPartSeparator)
	return base64.URLEncoding.EncodeToString([]byte(raw)), nil
}
