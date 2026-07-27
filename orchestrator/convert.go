package orchestrator

import (
	"fmt"
	"math"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"

	lua "github.com/yuin/gopher-lua"
)

func luaToPackValue(value lua.LValue, seen map[*lua.LTable]bool, depth int) (any, error) {
	if depth > maxValueDepth {
		return nil, fmt.Errorf("value nesting exceeds %d", maxValueDepth)
	}
	if userdata, ok := value.(*lua.LUserData); ok {
		raw, ok := userdata.Value.(rawMessageValue)
		if !ok {
			return nil, fmt.Errorf("unsupported Lua userdata in pulp.pack")
		}
		return msgpack.RawMessage(append([]byte(nil), raw...)), nil
	}
	table, ok := value.(*lua.LTable)
	if !ok {
		return luaToGo(value, seen, depth)
	}
	if seen[table] {
		return nil, fmt.Errorf("cyclic Lua table")
	}
	seen[table] = true
	defer delete(seen, table)

	type entry struct {
		key   lua.LValue
		value lua.LValue
	}
	var entries []entry
	table.ForEach(func(key, item lua.LValue) {
		entries = append(entries, entry{key: key, value: item})
	})
	if len(entries) == 0 {
		return map[string]any{}, nil
	}
	array := make([]any, len(entries))
	isArray := true
	for _, item := range entries {
		number, numberKey := item.key.(lua.LNumber)
		index := int(number)
		if !numberKey || float64(number) != float64(index) || index < 1 || index > len(entries) {
			isArray = false
			break
		}
		converted, err := luaToPackValue(item.value, seen, depth+1)
		if err != nil {
			return nil, err
		}
		array[index-1] = converted
	}
	if isArray {
		return array, nil
	}
	object := make(map[string]any, len(entries))
	for _, item := range entries {
		key, stringKey := item.key.(lua.LString)
		if !stringKey {
			return nil, fmt.Errorf("object key %s is not a string", item.key.Type())
		}
		converted, err := luaToPackValue(item.value, seen, depth+1)
		if err != nil {
			return nil, err
		}
		object[string(key)] = converted
	}
	return object, nil
}

const (
	maxValueDepth       = 64
	maxExactLuaInteger  = int64(1<<53 - 1)
	minExactLuaInteger  = -maxExactLuaInteger
	maxExactLuaUnsigned = uint64(maxExactLuaInteger)
)

func luaToGo(value lua.LValue, seen map[*lua.LTable]bool, depth int) (any, error) {
	if depth > maxValueDepth {
		return nil, fmt.Errorf("value nesting exceeds %d", maxValueDepth)
	}
	switch typed := value.(type) {
	case *lua.LNilType:
		return nil, nil
	case lua.LBool:
		return bool(typed), nil
	case lua.LNumber:
		return numberToGo(typed), nil
	case lua.LString:
		return string(typed), nil
	case *lua.LTable:
		if seen[typed] {
			return nil, fmt.Errorf("cyclic Lua table")
		}
		seen[typed] = true
		defer delete(seen, typed)

		type entry struct {
			key   lua.LValue
			value lua.LValue
		}
		var entries []entry
		typed.ForEach(func(key, item lua.LValue) {
			entries = append(entries, entry{key: key, value: item})
		})
		if len(entries) == 0 {
			return map[string]any{}, nil
		}

		array := make([]any, len(entries))
		isArray := true
		for _, item := range entries {
			number, ok := item.key.(lua.LNumber)
			index := int(number)
			if !ok || float64(number) != float64(index) || index < 1 || index > len(entries) {
				isArray = false
				break
			}
			converted, err := luaToGo(item.value, seen, depth+1)
			if err != nil {
				return nil, err
			}
			array[index-1] = converted
		}
		if isArray {
			return array, nil
		}

		object := make(map[string]any, len(entries))
		for _, item := range entries {
			key, ok := item.key.(lua.LString)
			if !ok {
				return nil, fmt.Errorf("object key %s is not a string", item.key.Type())
			}
			converted, err := luaToGo(item.value, seen, depth+1)
			if err != nil {
				return nil, err
			}
			object[string(key)] = converted
		}
		return object, nil
	default:
		return nil, fmt.Errorf("unsupported Lua value type %s", value.Type())
	}
}

func goToLua(l *lua.LState, value any, depth int) (lua.LValue, error) {
	if depth > maxValueDepth {
		return nil, fmt.Errorf("value nesting exceeds %d", maxValueDepth)
	}
	if value == nil {
		return lua.LNil, nil
	}
	switch typed := value.(type) {
	case bool:
		return lua.LBool(typed), nil
	case string:
		return lua.LString(typed), nil
	case []byte:
		return lua.LString(string(typed)), nil
	case int:
		return signedIntegerToLua(int64(typed))
	case int8:
		return signedIntegerToLua(int64(typed))
	case int16:
		return signedIntegerToLua(int64(typed))
	case int32:
		return signedIntegerToLua(int64(typed))
	case int64:
		return signedIntegerToLua(typed)
	case uint:
		return unsignedIntegerToLua(uint64(typed))
	case uint8:
		return unsignedIntegerToLua(uint64(typed))
	case uint16:
		return unsignedIntegerToLua(uint64(typed))
	case uint32:
		return unsignedIntegerToLua(uint64(typed))
	case uint64:
		return unsignedIntegerToLua(typed)
	case float32:
		return floatToLua(float64(typed))
	case float64:
		return floatToLua(typed)
	case map[string]any:
		table := l.NewTable()
		for key, item := range typed {
			converted, err := goToLua(l, item, depth+1)
			if err != nil {
				return nil, err
			}
			table.RawSetString(key, converted)
		}
		return table, nil
	case map[any]any:
		table := l.NewTable()
		for key, item := range typed {
			stringKey, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("map key %T is not a string", key)
			}
			converted, err := goToLua(l, item, depth+1)
			if err != nil {
				return nil, err
			}
			table.RawSetString(stringKey, converted)
		}
		return table, nil
	case []any:
		table := l.NewTable()
		for _, item := range typed {
			converted, err := goToLua(l, item, depth+1)
			if err != nil {
				return nil, err
			}
			table.Append(converted)
		}
		return table, nil
	}

	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Slice, reflect.Array:
		table := l.NewTable()
		for i := 0; i < reflected.Len(); i++ {
			converted, err := goToLua(l, reflected.Index(i).Interface(), depth+1)
			if err != nil {
				return nil, err
			}
			table.Append(converted)
		}
		return table, nil
	case reflect.Map:
		table := l.NewTable()
		iterator := reflected.MapRange()
		for iterator.Next() {
			if iterator.Key().Kind() != reflect.String {
				return nil, fmt.Errorf("map key %s is not a string", iterator.Key().Kind())
			}
			converted, err := goToLua(l, iterator.Value().Interface(), depth+1)
			if err != nil {
				return nil, err
			}
			table.RawSetString(iterator.Key().String(), converted)
		}
		return table, nil
	default:
		return nil, fmt.Errorf("unsupported Go value type %T", value)
	}
}

func signedIntegerToLua(value int64) (lua.LValue, error) {
	if value < minExactLuaInteger || value > maxExactLuaInteger {
		return nil, fmt.Errorf(
			"integer %d is outside Lua's exact integer range [%d, %d]; encode large IDs and counters as strings",
			value, minExactLuaInteger, maxExactLuaInteger,
		)
	}
	return lua.LNumber(value), nil
}

func unsignedIntegerToLua(value uint64) (lua.LValue, error) {
	if value > maxExactLuaUnsigned {
		return nil, fmt.Errorf(
			"integer %d is outside Lua's exact integer range [0, %d]; encode large IDs and counters as strings",
			value, maxExactLuaUnsigned,
		)
	}
	return lua.LNumber(value), nil
}

func floatToLua(value float64) (lua.LValue, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, fmt.Errorf("non-finite float %v is not supported by Lua workflow values", value)
	}
	return lua.LNumber(value), nil
}
