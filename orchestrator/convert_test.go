package orchestrator

import (
	"math"
	"strings"
	"testing"

	lua "github.com/yuin/gopher-lua"
)

func TestGoToLuaAcceptsExactIntegerBoundaries(t *testing.T) {
	state := lua.NewState()
	defer state.Close()

	tests := []struct {
		name  string
		value any
		want  lua.LNumber
	}{
		{name: "signed minimum", value: minExactLuaInteger, want: lua.LNumber(minExactLuaInteger)},
		{name: "signed maximum", value: maxExactLuaInteger, want: lua.LNumber(maxExactLuaInteger)},
		{name: "unsigned maximum", value: maxExactLuaUnsigned, want: lua.LNumber(maxExactLuaUnsigned)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := goToLua(state, test.value, 0)
			if err != nil {
				t.Fatalf("goToLua(%v): %v", test.value, err)
			}
			if got != test.want {
				t.Fatalf("goToLua(%v) = %#v, want %#v", test.value, got, test.want)
			}
		})
	}
}

func TestGoToLuaRejectsInexactIntegers(t *testing.T) {
	state := lua.NewState()
	defer state.Close()

	tests := []struct {
		name  string
		value any
	}{
		{name: "signed below", value: minExactLuaInteger - 1},
		{name: "signed above", value: maxExactLuaInteger + 1},
		{name: "unsigned above", value: maxExactLuaUnsigned + 1},
		{name: "maximum int64", value: int64(math.MaxInt64)},
		{name: "maximum uint64", value: uint64(math.MaxUint64)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := goToLua(state, test.value, 0)
			if err == nil || !strings.Contains(err.Error(), "outside Lua's exact integer range") {
				t.Fatalf("goToLua(%v) error = %v", test.value, err)
			}
		})
	}
}

func TestGoToLuaRejectsNonFiniteFloats(t *testing.T) {
	state := lua.NewState()
	defer state.Close()

	tests := []any{
		math.NaN(),
		math.Inf(1),
		math.Inf(-1),
		float32(math.Inf(1)),
	}
	for _, value := range tests {
		_, err := goToLua(state, value, 0)
		if err == nil || !strings.Contains(err.Error(), "non-finite float") {
			t.Fatalf("goToLua(%v) error = %v", value, err)
		}
	}

	if _, err := goToLua(state, math.MaxFloat64, 0); err != nil {
		t.Fatalf("finite float rejected: %v", err)
	}
}
