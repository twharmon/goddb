package goddb

import (
	"fmt"
)

type DeleteAllRequest[T any] struct {
	value        *T
	beginsWith   *T
	betweenStart *T
	betweenEnd   *T
}

func DeleteAll[T any](v *T) *DeleteAllRequest[T] {
	return &DeleteAllRequest[T]{
		value: v,
	}
}

func (r *DeleteAllRequest[T]) BeginsWith(v *T) *DeleteAllRequest[T] {
	r.beginsWith = v
	return r
}

func (r *DeleteAllRequest[T]) Between(start *T, end *T) *DeleteAllRequest[T] {
	r.betweenStart = start
	r.betweenEnd = end
	return r
}

func (r *DeleteAllRequest[T]) Exec() error {
	wrap := func(err error) error {
		return fmt.Errorf("goddb delete all: %w", err)
	}
	values, err := Query(r.value).BeginsWith(r.beginsWith).Between(r.betweenStart, r.betweenEnd).Exec()
	if err != nil {
		return wrap(err)
	}
	for _, value := range values {
		if err := Delete(value).Exec(); err != nil {
			return wrap(err)
		}
	}
	return nil
}
