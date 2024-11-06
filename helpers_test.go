package goddb_test

import (
	"strings"
	"testing"
)

func assertEq[T comparable](t *testing.T, a T, b T) {
	if a != b {
		t.Fatalf("%v != %v", a, b)
	}
}

func assertBeginsWith(t *testing.T, a string, b string) {
	if !strings.HasPrefix(a, b) {
		t.Fatalf("%v doesn't begin with %v", a, b)
	}
}

func assertContains(t *testing.T, a string, b string) {
	if !strings.Contains(a, b) {
		t.Fatalf("%v doesn't contain %v", a, b)
	}
}

func assertNe[T comparable](t *testing.T, a T, b T) {
	if a == b {
		t.Fatalf("%v == %v", a, b)
	}
}

func assertLte[T interface {
	int | string
}](t *testing.T, a T, b T) {
	if a > b {
		t.Fatalf("%v > %v", a, b)
	}
}

func assertGte[T interface {
	int | string
}](t *testing.T, a T, b T) {
	if b > a {
		t.Fatalf("%v < %v", a, b)
	}
}
