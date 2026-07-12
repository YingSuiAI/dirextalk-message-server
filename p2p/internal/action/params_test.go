package action

import (
	"encoding/json"
	"reflect"
	"testing"
)

type stringValue string

func (v stringValue) String() string { return string(v) }

func TestParamsCompatibilityReaders(t *testing.T) {
	params := Params{
		"string":      "  value  ",
		"stringer":    stringValue("  rendered  "),
		"bool_text":   " TRUE ",
		"bool_number": float64(2),
		"int_text":    " 42 ",
		"int_json":    json.Number("43"),
		"strings":     []any{" first ", stringValue("second"), "first", ""},
		"int64s":      []any{json.Number("5"), "6", 0, "invalid"},
		"bools":       map[string]any{" enabled ": "1", "disabled": false, "": true},
	}

	if got := params.String("string"); got != "value" {
		t.Fatalf("String() = %q, want value", got)
	}
	if got := params.String("stringer"); got != "rendered" {
		t.Fatalf("String() for fmt.Stringer = %q, want rendered", got)
	}
	if !params.Bool("bool_text") || !params.Bool("bool_number") {
		t.Fatal("Bool() did not preserve current string/number compatibility")
	}
	if got := params.Int64("int_text"); got != 42 {
		t.Fatalf("Int64() for text = %d, want 42", got)
	}
	if got := params.Int64("int_json"); got != 43 {
		t.Fatalf("Int64() for json.Number = %d, want 43", got)
	}
	if got, want := params.Strings("strings"), []string{"first", "second"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Strings() = %#v, want %#v", got, want)
	}
	if got, want := params.Int64s("int64s"), []int64{5, 6}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Int64s() = %#v, want %#v", got, want)
	}
	if got, want := params.BoolMap("bools"), map[string]bool{"enabled": true, "disabled": false}; !reflect.DeepEqual(got, want) {
		t.Fatalf("BoolMap() = %#v, want %#v", got, want)
	}
	if got := params.Raw("missing"); got != nil {
		t.Fatalf("Raw(missing) = %#v, want nil", got)
	}
}

func TestValueReadersPreserveInvalidAndScalarBehavior(t *testing.T) {
	if got := String(123); got != "" {
		t.Fatalf("String(number) = %q, want empty", got)
	}
	if got := Strings("one"); got != nil {
		t.Fatalf("Strings(scalar) = %#v, want nil", got)
	}
	if got := Int64(json.Number("invalid")); got != 0 {
		t.Fatalf("Int64(invalid) = %d, want zero", got)
	}
	if got := Int64s(int64(7)); !reflect.DeepEqual(got, []int64{7}) {
		t.Fatalf("Int64s(scalar) = %#v, want [7]", got)
	}
	if got := Int64s([]int64{0, 7}); !reflect.DeepEqual(got, []int64{0, 7}) {
		t.Fatalf("Int64s([]int64) = %#v, want an unchanged copy", got)
	}
	if Bool("false") || Bool(0) || Bool(nil) {
		t.Fatal("Bool() accepted false-compatible values")
	}
	if got := BoolMap(nil); got != nil {
		t.Fatalf("BoolMap(nil) = %#v, want nil", got)
	}
}
