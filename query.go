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

type QueryRequest[T any] struct {
	item         *T
	limit        int
	beginsWith   *T
	betweenStart *T
	betweenEnd   *T
	offset       *string
	consistent   bool
}

func Query[T any](item *T) *QueryRequest[T] {
	return &QueryRequest[T]{
		item: item,
	}
}

func (r *QueryRequest[T]) BeginsWith(v *T) *QueryRequest[T] {
	r.beginsWith = v
	return r
}

func (r *QueryRequest[T]) Between(start *T, end *T) *QueryRequest[T] {
	r.betweenStart = start
	r.betweenEnd = end
	return r
}

func (r *QueryRequest[T]) Page(maxSize int, offset *string) *QueryRequest[T] {
	r.limit = maxSize
	r.offset = offset
	return r
}

func (r *QueryRequest[T]) Consistent() *QueryRequest[T] {
	r.consistent = true
	return r
}

func (r *QueryRequest[T]) Exec() ([]*T, error) {
	if r.betweenStart != nil {
		return r.execBetween()
	}
	return r.execBeginsWith()
}

func (r *QueryRequest[T]) execBeginsWith() ([]*T, error) {
	wrap := func(err error) error {
		return fmt.Errorf("goddb query: %w", err)
	}
	pkVal, err := valueOf(r.item)
	if err != nil {
		return nil, wrap(err)
	}
	if r.beginsWith == nil {
		r.beginsWith = new(T)
	}
	pkType := pkVal.Type()
	pkitem, err := makeItem(pkType, pkVal, func(attr string) bool {
		return strings.HasSuffix(attr, "PK") || strings.HasSuffix(attr, "GSI")
	})
	if err != nil {
		return nil, wrap(err)
	}
	index, err := r.chooseIndex(pkitem, pkVal, pkType)
	if err != nil {
		return nil, wrap(err)
	}
	if index == pkType.Name()+"GSI" {
		result, err := r.scan(index)
		if err != nil {
			return nil, wrap(err)
		}
		return result, nil
	}
	skval, err := valueOf(r.beginsWith)
	if err != nil {
		return nil, wrap(err)
	}
	skitem, err := makeItem(skval.Type(), skval, func(attr string) bool {
		return strings.HasSuffix(attr, "SK")
	})
	if err != nil {
		return nil, wrap(err)
	}
	input := &dynamodb.QueryInput{
		TableName: aws.String(os.Getenv("GODDB_TABLE_NAME")),
	}
	if r.consistent {
		input.ConsistentRead = aws.Bool(true)
	}
	if r.limit > 0 {
		input.Limit = aws.Int32(int32(r.limit))
	}
	if index != "" {
		input.IndexName = &index
	}
	if input.ExpressionAttributeNames == nil {
		input.ExpressionAttributeNames = make(map[string]string)
	}
	input.ExpressionAttributeNames["#pk"] = index + "PK"
	input.ExpressionAttributeNames["#sk"] = index + "SK"
	pkattrval, ok := pkitem[index+"PK"]
	if !ok {
		return nil, wrap(fmt.Errorf("could not get hash key from index %s", index))
	}
	skattrval, ok := skitem[index+"SK"]
	if !ok {
		return nil, wrap(errors.New("could not get range key"))
	}
	pkmember, ok := pkattrval.(*types.AttributeValueMemberS)
	if !ok {
		return nil, wrap(errors.New("hash attribute value not of type string"))
	}
	skmember, ok := skattrval.(*types.AttributeValueMemberS)
	if !ok {
		return nil, wrap(errors.New("range attribute value not of type string"))
	}
	if input.ExpressionAttributeValues == nil {
		input.ExpressionAttributeValues = make(map[string]types.AttributeValue)
	}
	input.ExpressionAttributeValues[":pk"] = &types.AttributeValueMemberS{Value: pkmember.Value}
	input.ExpressionAttributeValues[":sk"] = &types.AttributeValueMemberS{Value: skmember.Value}
	input.KeyConditionExpression = aws.String("#pk = :pk and begins_with(#sk, :sk)")
	result, err := r.exec(input)
	if err != nil {
		return nil, wrap(err)
	}
	return result, nil
}

func (r *QueryRequest[T]) exec(input *dynamodb.QueryInput) ([]*T, error) {
	var lek map[string]types.AttributeValue
	if r.offset != nil {
		var err error
		lek, err = offsetToLastEvaluatedKey(*r.offset)
		if err != nil {
			return nil, err
		}
	}
	var result []*T
	ctx := context.Background()
	for {
		input.ExclusiveStartKey = lek
		output, err := client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		lek = output.LastEvaluatedKey
		vals, err := loadValues[T](output.Items)
		if err != nil {
			return nil, err
		}
		result = append(result, vals...)
		if lek == nil {
			if r.offset != nil {
				*r.offset = ""
			}
			break
		}
		if r.offset != nil {
			*r.offset, err = lastEvaluatedKeyToOffset(lek)
			if err != nil {
				return nil, err
			}
		}
		if input.Limit != nil {
			break
		}
	}
	return result, nil
}

func (r *QueryRequest[T]) scan(index string) ([]*T, error) {
	var lek map[string]types.AttributeValue
	if r.offset != nil {
		var err error
		lek, err = offsetToLastEvaluatedKey(*r.offset)
		if err != nil {
			return nil, err
		}
	}
	var result []*T
	ctx := context.Background()
	input := &dynamodb.ScanInput{
		TableName: aws.String(os.Getenv("GODDB_TABLE_NAME")),
		IndexName: &index,
	}
	if r.limit > 0 {
		input.Limit = aws.Int32(int32(r.limit))
	}
	if r.consistent {
		input.ConsistentRead = aws.Bool(true)
	}
	for {
		input.ExclusiveStartKey = lek
		output, err := client.Scan(ctx, input)
		if err != nil {
			return nil, err
		}
		lek = output.LastEvaluatedKey
		vals, err := loadValues[T](output.Items)
		if err != nil {
			return nil, err
		}
		result = append(result, vals...)
		if lek == nil {
			if r.offset != nil {
				*r.offset = ""
			}
			break
		}
		if r.offset != nil {
			*r.offset, err = lastEvaluatedKeyToOffset(lek)
			if err != nil {
				return nil, err
			}
		}
		if input.Limit != nil {
			break
		}
	}
	return result, nil
}

func (r *QueryRequest[T]) chooseIndex(item map[string]types.AttributeValue, val reflect.Value, ty reflect.Type) (string, error) {
	attrToFields := make(map[string][]string)
	for i := 0; i < ty.NumField(); i++ {
		ft := ty.Field(i)
		if !ft.IsExported() {
			continue
		}
		if tag := ft.Tag.Get("goddb"); tag != "" {
			attrs := strings.Split(tag, ",")
			for _, attr := range attrs {
				attrToFields[attr] = append(attrToFields[attr], ft.Name)
			}
		}
	}
	pks := make(map[string]int)
	var gsi string
	// TODO: put require at most 1 simple gsi
	for attrName, attrVal := range item {
		if strings.HasSuffix(attrName, "PK") {
			member, ok := attrVal.(*types.AttributeValueMemberS)
			if !ok {
				return "", errors.New("hash attribute not string")
			}
			if strings.HasSuffix(member.Value, string(TagChar)) {
				continue
			}
			var foundZeroValue bool
			for _, field := range attrToFields[attrName] {
				if val.FieldByName(field).IsZero() {
					foundZeroValue = true
					break
				}
			}
			if foundZeroValue {
				continue
			}
			pks[strings.TrimSuffix(attrName, "PK")] = len(strings.Split(member.Value, string(TagChar)))
		}
		if attrName == ty.Name()+"GSI" {
			gsi = attrName
		}
	}
	var maxPKIndexes []string
	var maxPKCnt int
	for k, v := range pks {
		if v > maxPKCnt {
			maxPKCnt = v
			maxPKIndexes = []string{k}
		} else if v == maxPKCnt {
			maxPKIndexes = append(maxPKIndexes, k)
		}
	}
	if len(maxPKIndexes) == 0 {
		if gsi != "" {
			return gsi, nil
		}
		return "", errors.New("unable to locate index")
	}
	if len(maxPKIndexes) > 1 {
		return "", errors.New("ambiguous index")
	}
	return maxPKIndexes[0], nil
}

func (r *QueryRequest[T]) execBetween() ([]*T, error) {
	wrap := func(err error) error {
		return fmt.Errorf("goddb query: %w", err)
	}
	pkval, err := valueOf(r.item)
	if err != nil {
		return nil, wrap(err)
	}
	if r.beginsWith == nil {
		r.beginsWith = new(T)
	}
	startval, err := valueOf(r.betweenStart)
	if err != nil {
		return nil, wrap(err)
	}
	endval, err := valueOf(r.betweenEnd)
	if err != nil {
		return nil, wrap(err)
	}
	pkType := pkval.Type()
	pkitem, err := makeItem(pkType, pkval, func(attr string) bool {
		return strings.HasSuffix(attr, "PK") || strings.HasSuffix(attr, "GSI")
	})
	if err != nil {
		return nil, wrap(err)
	}
	index, err := r.chooseIndex(pkitem, pkval, pkType)
	if err != nil {
		return nil, wrap(err)
	}
	if index == pkType.Name()+"GSI" {
		result, err := r.scan(index)
		if err != nil {
			return nil, wrap(err)
		}
		return result, nil
	}
	startItem, err := makeItem(startval.Type(), startval, func(attr string) bool {
		return strings.HasSuffix(attr, "SK")
	})
	if err != nil {
		return nil, wrap(err)
	}
	endItem, err := makeItem(endval.Type(), endval, func(attr string) bool {
		return strings.HasSuffix(attr, "SK")
	})
	if err != nil {
		return nil, wrap(err)
	}
	input := &dynamodb.QueryInput{
		TableName: aws.String(os.Getenv("GODDB_TABLE_NAME")),
	}
	if r.limit > 0 {
		input.Limit = aws.Int32(int32(r.limit))
	}
	if r.consistent {
		input.ConsistentRead = aws.Bool(true)
	}
	if index != "" {
		input.IndexName = &index
	}
	if input.ExpressionAttributeNames == nil {
		input.ExpressionAttributeNames = make(map[string]string)
	}
	input.ExpressionAttributeNames["#pk"] = index + "PK"
	input.ExpressionAttributeNames["#sk"] = index + "SK"
	pkattrval, ok := pkitem[index+"PK"]
	if !ok {
		return nil, wrap(errors.New("could not get hash key"))
	}
	startAttrVal, ok := startItem[index+"SK"]
	if !ok {
		return nil, wrap(errors.New("could not get range key"))
	}
	endAttrVal, ok := endItem[index+"SK"]
	if !ok {
		return nil, wrap(errors.New("could not get range key"))
	}
	pkmember, ok := pkattrval.(*types.AttributeValueMemberS)
	if !ok {
		return nil, wrap(errors.New("hash attribute value not of type string"))
	}
	startMember, ok := startAttrVal.(*types.AttributeValueMemberS)
	if !ok {
		return nil, wrap(errors.New("range attribute value not of type string"))
	}
	endMember, ok := endAttrVal.(*types.AttributeValueMemberS)
	if !ok {
		return nil, wrap(errors.New("range attribute value not of type string"))
	}
	if input.ExpressionAttributeValues == nil {
		input.ExpressionAttributeValues = make(map[string]types.AttributeValue)
	}
	input.ExpressionAttributeValues[":pk"] = &types.AttributeValueMemberS{Value: pkmember.Value}
	input.ExpressionAttributeValues[":start"] = &types.AttributeValueMemberS{Value: startMember.Value}
	input.ExpressionAttributeValues[":end"] = &types.AttributeValueMemberS{Value: endMember.Value}
	input.KeyConditionExpression = aws.String("#pk = :pk and #sk between :start and :end")
	return r.exec(input)
}
