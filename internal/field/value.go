package field

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/Huangkai1008/libradb/pkg/typing"
)

var (
	ErrValueNil         = errors.New("value cannot be nil")
	ErrByteSizeMismatch = errors.New("bytesize mismatch")
)

type Value interface {
	Type() Type
	Val() any
	ToBytes() []byte
	typing.Comparable[Value]
}

func NewValue(t Type, val any) Value {
	switch t.TypeID() {
	case INTEGER:
		return IntegerValue{t: t.(*Integer), val: int32(val.(int))}
	case VARCHAR:
		return VarcharValue{t: t.(*Varchar), val: val.(string)}
	case BOOLEAN:
		return BooleanValue{t: t.(*Boolean), val: val.(bool)}
	case FLOAT:
		return FloatValue{t: t.(*Float), val: float32(val.(float64))}
	default:
		panic("not implemented")
	}
}

func IsNull(v Value) bool {
	return v == nil
}

func ByteSize(v Value) int {
	if IsVarLen(v.Type()) {
		perByteSize := v.Type().ByteSize() / Length(v.Type())
		return perByteSize * utf8.RuneCountInString(v.Val().(string))
	}
	return v.Type().ByteSize()
}

func FromBytes(t Type, bytes []byte) (Value, error) {
	if !IsVarLen(t) && len(bytes) != t.ByteSize() {
		return nil, ErrByteSizeMismatch
	}

	switch t.TypeID() {
	case INTEGER:
		val := int32(binary.LittleEndian.Uint32(bytes))
		return IntegerValue{t: t.(*Integer), val: val}, nil
	case VARCHAR:
		runes := make([]rune, 0, len(bytes)/4) //nolint:mnd // 4 bytes per rune
		for i := 0; i < len(bytes); i += 4 {
			runes = append(runes, rune(binary.LittleEndian.Uint32(bytes[i:])))
		}
		return VarcharValue{t: t.(*Varchar), val: string(runes)}, nil
	case BOOLEAN:
		val := bytes[0] == 1
		return BooleanValue{t: t.(*Boolean), val: val}, nil
	case FLOAT:
		val := math.Float32frombits(binary.LittleEndian.Uint32(bytes))
		return FloatValue{t: t.(*Float), val: val}, nil
	}
	return nil, ErrValueNil
}

type IntegerValue struct {
	t   *Integer
	val int32
}

func (v IntegerValue) Compare(t Value) int {
	return cmp.Compare(v.val, t.(IntegerValue).val)
}

func (v IntegerValue) Type() Type {
	return v.t
}

func (v IntegerValue) Val() any {
	return v.val
}

func (v IntegerValue) ToBytes() []byte {
	bytes := make([]byte, v.Type().ByteSize())
	binary.LittleEndian.PutUint32(bytes, uint32(v.val))
	return bytes
}

func (v IntegerValue) String() string {
	return strconv.Itoa(int(v.val))
}

type VarcharValue struct {
	t   *Varchar
	val string
}

func (v VarcharValue) Compare(t Value) int {
	return cmp.Compare(v.val, t.(VarcharValue).val)
}

func (v VarcharValue) Type() Type {
	return v.t
}

func (v VarcharValue) Val() any {
	return v.val
}

func (v VarcharValue) ToBytes() []byte {
	runes := []rune(v.val)
	charCount := len(runes)
	bytes := make([]byte, charCount*4) //nolint:mnd // 4 bytes per rune
	for i, r := range runes {
		binary.LittleEndian.PutUint32(bytes[i*4:], uint32(r))
	}
	return bytes
}

func (v VarcharValue) String() string {
	return v.val
}

type BooleanValue struct {
	t   *Boolean
	val bool
}

func (v BooleanValue) Compare(t Value) int {
	if v.val == t.(BooleanValue).val {
		return 0
	}
	if v.val {
		return 1
	}
	return -1
}

func (v BooleanValue) Type() Type {
	return v.t
}

func (v BooleanValue) Val() any {
	return v.val
}

func (v BooleanValue) ToBytes() []byte {
	bytes := make([]byte, 1)
	if v.val {
		bytes[0] = 1
	}
	return bytes
}

func (v BooleanValue) String() string {
	return strconv.FormatBool(v.val)
}

type FloatValue struct {
	t   *Float
	val float32
}

func (v FloatValue) Compare(t Value) int {
	return cmp.Compare(v.val, t.(FloatValue).val)
}

func (v FloatValue) Type() Type {
	return v.t
}

func (v FloatValue) Val() any {
	return v.val
}

func (v FloatValue) ToBytes() []byte {
	bytes := make([]byte, 4) //nolint:mnd // 4 bytes per float32
	binary.LittleEndian.PutUint32(bytes, math.Float32bits(v.val))
	return bytes
}

func (v FloatValue) String() string {
	return fmt.Sprint(v.val)
}
