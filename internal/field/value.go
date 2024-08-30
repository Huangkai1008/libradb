package field

import (
	"bytes"
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
	case BINARY:
		return BinaryValue{t: t.(*Binary), val: val.([]byte)}
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
	case BINARY:
		return BinaryValue{t: t.(*Binary), val: bytes}, nil
	default:
		return nil, ErrValueNil
	}
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
	b := make([]byte, v.Type().ByteSize())
	binary.LittleEndian.PutUint32(b, uint32(v.val))
	return b
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
	b := make([]byte, charCount*4) //nolint:mnd // 4 b per rune
	for i, r := range runes {
		binary.LittleEndian.PutUint32(b[i*4:], uint32(r))
	}
	return b
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
	b := make([]byte, 1)
	if v.val {
		b[0] = 1
	}
	return b
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
	b := make([]byte, 4) //nolint:mnd // 4 b per float32
	binary.LittleEndian.PutUint32(b, math.Float32bits(v.val))
	return b
}

func (v FloatValue) String() string {
	return fmt.Sprint(v.val)
}

type BinaryValue struct {
	t   *Binary
	val []byte
}

func (v BinaryValue) Compare(t Value) int {
	return bytes.Compare(v.val, t.(BinaryValue).val)
}

func (v BinaryValue) Type() Type {
	return v.t
}

func (v BinaryValue) Val() any {
	return v.val
}

func (v BinaryValue) ToBytes() []byte {
	return v.val
}
