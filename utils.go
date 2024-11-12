package goddb

import (
	"cmp"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var valueTimeNonZero = time.Now()

var typeTime = reflect.TypeOf(valueTimeNonZero)

const timeFormat = "2006-01-02T15:04:05.000000000Z07:00"

var errHeterogenousSlice = errors.New("slice items must all be of the same, non-interface type")

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
	case reflect.Bool:
		return &types.AttributeValueMemberBOOL{Value: v.Bool()}, nil
	case reflect.Slice:
		return makeSliceAttributeValue(v)
	case reflect.Struct:
		iface := v.Interface()
		switch v.Type() {
		case typeTime:
			t := iface.(time.Time)
			return &types.AttributeValueMemberS{Value: t.UTC().Format(timeFormat)}, nil
		default:
			return nil, fmt.Errorf("unsupported type %T", iface)
		}
	default:
		return nil, fmt.Errorf("unsupported type %T", v.Interface())
	}
}

func makeSliceAttributeValue(v reflect.Value) (types.AttributeValue, error) {
	if v.Len() == 0 {
		return nil, nil
	}
	var consume func(reflect.Value) error
	var av types.AttributeValue
	first := v.Index(0)
	switch first.Kind() {
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
	case reflect.Struct:
		switch first.Type() {
		case typeTime:
			av = &types.AttributeValueMemberSS{}
			consume = func(v reflect.Value) error {
				if v.Type() != typeTime {
					return errHeterogenousSlice
				}
				ss := av.(*types.AttributeValueMemberSS)
				t := v.Interface().(time.Time)
				ss.Value = append(ss.Value, t.UTC().Format(timeFormat))
				return nil
			}
		default:
			return nil, fmt.Errorf("set items can only be of type string, number, or time.Time; found %T", first.Interface())
		}
	default:
		return nil, fmt.Errorf("set items can only be of type string, number, or time.Time; found %T", first.Interface())
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
			str := value.String()
			if strings.Contains(str, string(TagChar)) {
				return nil, fmt.Errorf("indexed values can not contain tag char %s", string(TagChar))
			}
			b.WriteString(str)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.WriteString(strconv.FormatInt(value.Int(), 10))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.WriteString(strconv.FormatUint(value.Uint(), 10))
		case reflect.Float32, reflect.Float64:
			b.WriteString(formatFloat(value.Float()))
		case reflect.Struct:
			iface := value.Interface()
			switch value.Type() {
			case typeTime:
				t := iface.(time.Time)
				b.WriteString(t.UTC().Format(timeFormat))
			default:
				return nil, fmt.Errorf("unsupported type %T", iface)
			}
		default:
			return nil, fmt.Errorf("unsupported type %T", value.Interface())
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

func validateCompleteKey(ty reflect.Type, val reflect.Value) error {
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
						return fmt.Errorf("found more than one field with sort key %s", attr)
					}
				}
			}
			if val.Field(i).IsZero() {
				return fmt.Errorf("field %s can not be zero value", ft.Name)
			}
		}
	}
	return nil
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
	case reflect.Bool:
		n, ok := v.(*types.AttributeValueMemberBOOL)
		if ok {
			fieldVal.SetBool(n.Value)
		}
	case reflect.Struct:
		switch fieldVal.Type() {
		case typeTime:
			s, ok := v.(*types.AttributeValueMemberS)
			if ok {
				t, err := time.Parse(timeFormat, s.Value)
				if err != nil {
					break
				}
				fieldVal.Set(reflect.ValueOf(t))
			}
		}
	case reflect.Slice:
		setSliceFieldValFromAttrVal(fieldVal, v)
	}
}

func setSliceFieldValFromAttrVal(fieldVal reflect.Value, v types.AttributeValue) error {
	elemType := fieldVal.Type().Elem()
	switch elemType.Kind() {
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
	case reflect.Struct:
		switch elemType {
		case typeTime:
			ss, ok := v.(*types.AttributeValueMemberSS)
			if !ok {
				return errors.New("wrong attribute type")
			}
			ts := make([]time.Time, len(ss.Value))
			var err error
			for i := range ts {
				ts[i], err = time.Parse(timeFormat, ss.Value[i])
				if err != nil {
					return err
				}
			}
			fieldVal.Set(reflect.ValueOf(ts))
		}
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
	case reflect.Struct:
		switch fieldVal.Type() {
		case typeTime:
			t, err := time.Parse(timeFormat, v)
			if err != nil {
				break
			}
			fieldVal.Set(reflect.ValueOf(t))
		}
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

func merge[K comparable, V any](a, b map[K]V) map[K]V {
	if len(b) == 0 {
		return a
	}
	if a == nil {
		a = make(map[K]V)
	}
	for k, v := range b {
		a[k] = v
	}
	return a
}
