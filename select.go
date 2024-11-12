package goddb

import "reflect"

func getFieldNameFromTest[T any](test func(*T) any) string {
	input := new(T)
	v := reflect.ValueOf(input).Elem()
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		ft := t.Field(i)
		if !ft.IsExported() {
			continue
		}
		fv := v.Field(i)
		if !fv.CanSet() {
			continue
		}
		switch fv.Kind() {
		case reflect.String:
			fv.SetString("a")
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fv.SetInt(1)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			fv.SetUint(1)
		case reflect.Bool:
			fv.SetBool(true)
		case reflect.Float32, reflect.Float64:
			fv.SetFloat(1)
		case reflect.Struct:
			switch fv.Type() {
			case typeTime:
				fv.Set(reflect.ValueOf(valueTimeNonZero))
			}
		}
		output := test(input)
		if !reflect.ValueOf(output).IsZero() {
			return ft.Name
		}
	}
	return ""
}
