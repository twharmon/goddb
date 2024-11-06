package goddb

import (
	"cmp"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var cfg = must(config.LoadDefaultConfig(context.Background()))
var client = dynamodb.NewFromConfig(cfg)
var TagChar = '#'

type Valuer interface {
	Value() types.AttributeValue
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func valueOf(t any) (reflect.Value, error) {
	val := reflect.ValueOf(t)
	for val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return reflect.Value{}, errors.New("must be struct")
	}
	return val, nil
}

var (
	typeStringSlice = reflect.TypeOf([]string{})
)

// makeAttributeValue can return nil, nil if it is empty
func makeAttributeValue(v reflect.Value) (types.AttributeValue, error) {
	switch v.Kind() {
	case reflect.String:
		return &types.AttributeValueMemberS{Value: v.String()}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return &types.AttributeValueMemberN{Value: strconv.FormatInt(v.Int(), 10)}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(v.Uint(), 10)}, nil
	case reflect.Float32, reflect.Float64:
		return &types.AttributeValueMemberN{Value: formatFloat(v.Float())}, nil
	case reflect.Slice:
		return makeSliceAttributeValue(v)
	default:
		iface := v.Interface()
		if stringer, ok := iface.(fmt.Stringer); ok {
			return &types.AttributeValueMemberS{Value: stringer.String()}, nil
		}
		return nil, fmt.Errorf("unsupported type %T", iface)
	}
}

var errHeterogenousSlice = errors.New("slice items must all be of the same, non-interface type")

func makeSliceAttributeValue(v reflect.Value) (types.AttributeValue, error) {
	if v.Len() == 0 {
		return nil, nil
	}
	var consume func(reflect.Value) error
	var av types.AttributeValue
	switch v.Index(0).Kind() {
	case reflect.String:
		av = &types.AttributeValueMemberSS{}
		consume = func(v reflect.Value) error {
			if v.Kind() != reflect.String {
				return errHeterogenousSlice
			}
			ss := av.(*types.AttributeValueMemberSS)
			ss.Value = append(ss.Value, v.String())
			return nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		av = &types.AttributeValueMemberNS{}
		consume = func(v reflect.Value) error {
			k := v.Kind()
			if k != reflect.Int && k != reflect.Int8 && k != reflect.Int16 && k != reflect.Int32 && k != reflect.Int64 {
				return errHeterogenousSlice
			}
			ns := av.(*types.AttributeValueMemberNS)
			ns.Value = append(ns.Value, strconv.FormatInt(v.Int(), 10))
			return nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		av = &types.AttributeValueMemberNS{}
		consume = func(v reflect.Value) error {
			k := v.Kind()
			if k != reflect.Uint && k != reflect.Uint8 && k != reflect.Uint16 && k != reflect.Uint32 && k != reflect.Uint64 {
				return errHeterogenousSlice
			}
			ns := av.(*types.AttributeValueMemberNS)
			ns.Value = append(ns.Value, strconv.FormatUint(v.Uint(), 10))
			return nil
		}
	case reflect.Float32, reflect.Float64:
		av = &types.AttributeValueMemberNS{}
		consume = func(v reflect.Value) error {
			k := v.Kind()
			if k != reflect.Float32 && k != reflect.Float64 {
				return errHeterogenousSlice
			}
			ns := av.(*types.AttributeValueMemberNS)
			ns.Value = append(ns.Value, formatFloat(v.Float()))
			return nil
		}
	default:
		return nil, fmt.Errorf("set items can only be of type string or number; found %T", v.Interface())
	}
	for i := 0; i < v.Len(); i++ {
		vv := v.Index(i)
		if err := consume(vv); err != nil {
			return nil, err
		}
	}
	return av, nil
}

func taggedAttributeValue(ps []tagValuePair) (types.AttributeValue, error) {
	slices.SortFunc(ps, func(a, b tagValuePair) int { return cmp.Compare(a.tag, b.tag) })
	var b strings.Builder
	for i := range ps {
		tag := ps[i].tag
		value := ps[i].value
		if b.Len() > 0 {
			b.WriteRune(TagChar)
		}
		if tag != "" {
			b.WriteString(tag)
			b.WriteRune(TagChar)
		}
		switch value.Kind() {
		case reflect.String:
			b.WriteString(value.String())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.WriteString(strconv.FormatInt(value.Int(), 10))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.WriteString(strconv.FormatUint(value.Uint(), 10))
		case reflect.Float32, reflect.Float64:
			b.WriteString(formatFloat(value.Float()))
		default:
			iface := value.Interface()
			if stringer, ok := iface.(fmt.Stringer); ok {
				b.WriteString(stringer.String())
			}
			return nil, fmt.Errorf("unsupported type %T", iface)
		}
	}
	return &types.AttributeValueMemberS{Value: b.String()}, nil
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func makeItem(ty reflect.Type, val reflect.Value, filter func(string) bool) (map[string]types.AttributeValue, error) {
	tagged := make(map[string][]tagValuePair)
	plain := make(map[string]int)
	for i := 0; i < ty.NumField(); i++ {
		f := ty.Field(i)
		if !f.IsExported() {
			continue
		}
		fv := val.Field(i)
		if tag := f.Tag.Get("goddb"); tag != "" {
			attrs := strings.Split(tag, ",")
			for _, attr := range attrs {
				if !filter(attr) {
					continue
				}
				var tag string
				if strings.HasSuffix(attr, "PK") {
					sk := attr[:len(attr)-2] + "SK"
					if slices.Contains(attrs, sk) {
						tag = ty.Name()
					} else {
						tag = f.Name
					}
				} else if strings.HasSuffix(attr, "SK") {
					tag = ty.Name()
				}
				tagged[attr] = append(tagged[attr], tagValuePair{
					tag:   tag,
					value: fv,
				})
			}
			continue
		}
		if filter(f.Name) {
			plain[f.Name] = i
		}
	}
	item := make(map[string]types.AttributeValue)
	for k, v := range plain {
		fv := val.Field(v)
		if fv.IsZero() {
			continue
		}
		av, err := makeAttributeValue(fv)
		if err != nil {
			return nil, err
		}
		if av == nil {
			continue
		}
		item[k] = av
	}
	for k, v := range tagged {
		av, err := taggedAttributeValue(v)
		if err != nil {
			return nil, err
		}
		item[k] = av
	}
	return item, nil
}

func setFieldValues(val reflect.Value, item map[string]types.AttributeValue) error {
	ty := val.Type()
	var skFieldVal reflect.Value
	for i := 0; i < ty.NumField(); i++ {
		f := ty.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := f.Tag.Get("goddb")
		attrs := strings.Split(tag, ",")
		if slices.Contains(attrs, "SK") {
			skFieldVal = val.Field(i)
			break
		}
	}
	for attrName, attrVal := range item {
		av := item[attrName]
		if strings.HasSuffix(attrName, "PK") {
			s, ok := av.(*types.AttributeValueMemberS)
			if !ok {
				return fmt.Errorf("attribute %s should be string", attrName)
			}
			parts := strings.Split(s.Value, string(TagChar))
			for i := 0; i < len(parts)/2; i++ {
				tag := parts[i*2]
				v := parts[i*2+1]
				fieldVal := val.FieldByName(tag)
				setFieldValFromVal(fieldVal, v)
			}
			continue
		}
		if strings.HasSuffix(attrName, "SK") {
			s, ok := av.(*types.AttributeValueMemberS)
			if !ok {
				return fmt.Errorf("attribute %s should be string", attrName)
			}
			parts := strings.Split(s.Value, string(TagChar))
			v := parts[1]
			setFieldValFromVal(skFieldVal, v)
			continue
		}
		setFieldValFromAttrVal(val.FieldByName(attrName), attrVal)
	}
	return nil
}

func setFieldValFromAttrVal(fieldVal reflect.Value, v types.AttributeValue) {
	if !fieldVal.IsValid() {
		return
	}
	switch fieldVal.Kind() {
	case reflect.String:
		s, ok := v.(*types.AttributeValueMemberS)
		if ok {
			fieldVal.SetString(s.Value)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, ok := v.(*types.AttributeValueMemberN)
		if ok {
			i, err := strconv.ParseInt(n.Value, 10, 64)
			if err != nil {
				break
			}
			fieldVal.SetInt(i)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, ok := v.(*types.AttributeValueMemberN)
		if ok {
			i, err := strconv.ParseUint(n.Value, 10, 64)
			if err != nil {
				break
			}
			fieldVal.SetUint(i)
		}
	case reflect.Float32, reflect.Float64:
		n, ok := v.(*types.AttributeValueMemberN)
		if ok {
			i, err := strconv.ParseFloat(n.Value, 64)
			if err != nil {
				break
			}
			fieldVal.SetFloat(i)
		}
	case reflect.Slice:
		setSliceFieldValFromAttrVal(fieldVal, v)
	}
}

func setSliceFieldValFromAttrVal(fieldVal reflect.Value, v types.AttributeValue) error {
	switch fieldVal.Type().Elem().Kind() {
	case reflect.String:
		ss, ok := v.(*types.AttributeValueMemberSS)
		if !ok {
			return errors.New("wrong attribute type")
		}
		fieldVal.Set(reflect.ValueOf(ss.Value))
	case reflect.Int:
		setIntSliceFieldValFromAttrVal[int](fieldVal, v)
	case reflect.Int8:
		setIntSliceFieldValFromAttrVal[int8](fieldVal, v)
	case reflect.Int16:
		setIntSliceFieldValFromAttrVal[int16](fieldVal, v)
	case reflect.Int32:
		setIntSliceFieldValFromAttrVal[int32](fieldVal, v)
	case reflect.Int64:
		setIntSliceFieldValFromAttrVal[int64](fieldVal, v)
	case reflect.Uint:
		setUintSliceFieldValFromAttrVal[uint](fieldVal, v)
	case reflect.Uint8:
		setUintSliceFieldValFromAttrVal[uint8](fieldVal, v)
	case reflect.Uint16:
		setUintSliceFieldValFromAttrVal[uint16](fieldVal, v)
	case reflect.Uint32:
		setUintSliceFieldValFromAttrVal[uint32](fieldVal, v)
	case reflect.Uint64:
		setUintSliceFieldValFromAttrVal[uint64](fieldVal, v)
	case reflect.Float32:
		setFloatSliceFieldValFromAttrVal[float32](fieldVal, v)
	case reflect.Float64:
		setFloatSliceFieldValFromAttrVal[float64](fieldVal, v)
	}
	return nil
}

func setIntSliceFieldValFromAttrVal[T interface {
	int | int64 | int32 | int16 | int8
}](fieldVal reflect.Value, v types.AttributeValue) error {
	ns, ok := v.(*types.AttributeValueMemberNS)
	if !ok {
		return errors.New("wrong attribute type")
	}
	vals := make([]T, len(ns.Value))
	for i := 0; i < len(ns.Value); i++ {
		val, err := strconv.ParseInt(ns.Value[i], 10, 64)
		if err != nil {
			return err
		}
		vals[i] = T(val)
	}
	fieldVal.Set(reflect.ValueOf(vals))
	return nil
}

func setUintSliceFieldValFromAttrVal[T interface {
	uint | uint64 | uint32 | uint16 | uint8
}](fieldVal reflect.Value, v types.AttributeValue) error {
	ns, ok := v.(*types.AttributeValueMemberNS)
	if !ok {
		return errors.New("wrong attribute type")
	}
	vals := make([]T, len(ns.Value))
	for i := 0; i < len(ns.Value); i++ {
		val, err := strconv.ParseUint(ns.Value[i], 10, 64)
		if err != nil {
			return err
		}
		vals[i] = T(val)
	}
	fieldVal.Set(reflect.ValueOf(vals))
	return nil
}

func setFloatSliceFieldValFromAttrVal[T interface {
	float32 | float64
}](fieldVal reflect.Value, v types.AttributeValue) error {
	ns, ok := v.(*types.AttributeValueMemberNS)
	if !ok {
		return errors.New("wrong attribute type")
	}
	vals := make([]T, len(ns.Value))
	for i := 0; i < len(ns.Value); i++ {
		val, err := strconv.ParseFloat(ns.Value[i], 64)
		if err != nil {
			return err
		}
		vals[i] = T(val)
	}
	fieldVal.Set(reflect.ValueOf(vals))
	return nil
}

func setFieldValFromVal(fieldVal reflect.Value, v string) {
	switch fieldVal.Kind() {
	case reflect.String:
		fieldVal.SetString(v)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			break
		}
		fieldVal.SetInt(i)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		i, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			break
		}
		fieldVal.SetUint(i)
	case reflect.Float32, reflect.Float64:
		i, err := strconv.ParseFloat(v, 64)
		if err != nil {
			break
		}
		fieldVal.SetFloat(i)
	}
}

func loadValues[T any](items []map[string]types.AttributeValue) ([]*T, error) {
	result := make([]*T, len(items))
	for i, item := range items {
		t := new(T)
		val := reflect.ValueOf(t)
		for val.Kind() == reflect.Pointer {
			val = val.Elem()
		}
		if err := setFieldValues(val, item); err != nil {
			return nil, err
		}
		result[i] = t
	}
	return result, nil
}

func getFieldNameFromTest[T any](test func(*T) any) string {
	input := new(T)
	v := reflect.ValueOf(input).Elem()
	t := v.Type()
	var strs int
	var ints int64
	var uints uint64
	var floats float64
	for i := 0; i < t.NumField(); i++ {
		ft := t.Field(i)
		if !ft.IsExported() {
			continue
		}
		fv := v.Field(i)
		switch fv.Kind() {
		case reflect.String:
			strs++
			fv.SetString(strconv.Itoa(strs))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			ints++
			fv.SetInt(ints)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			uints++
			fv.SetUint(uints)
		case reflect.Float32, reflect.Float64:
			floats++
			fv.SetFloat(floats)
		}
	}
	output := test(input)
	for i := 0; i < t.NumField(); i++ {
		ft := t.Field(i)
		if !ft.IsExported() {
			continue
		}
		fv := v.Field(i)
		if fv.Interface() == output {
			return ft.Name
		}
	}
	return ""
}

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
