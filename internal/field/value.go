package field

import (
	"cmp"
	"encoding/binary"
	"errors"
	"math"
	"unicode/utf8"
)

var (
	ErrValueNil         = errors.New("value cannot be nil")
	ErrBytesizeMismatch = errors.New("bytesize mismatch")
)

type Value interface {
	Type() Type
	Val() any
	ToBytes() []byte
}

func IsNull(v Value) bool {
	return v == nil
}

func Bytesize(v Value) int {
	if IsVarLen(v.Type()) {
		perByteSize := v.Type().ByteSize() / Length(v.Type())
		return perByteSize * utf8.RuneCountInString(v.Val().(string))
	}
	return v.Type().ByteSize()
}

func FromBytes(t Type, bytes []byte) (Value, error) {
	if !IsVarLen(t) && len(bytes) != t.ByteSize() {
		return nil, ErrBytesizeMismatch
	}

	switch t.TypeID() {
	case INTEGER:
		val := int32(binary.LittleEndian.Uint32(bytes))
		return IntegerValue{t: t.(*Integer), val: val}, nil
	case VARCHAR:
		runes := make([]rune, 0, len(bytes)/4)
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

func (v IntegerValue) Compare(t IntegerValue) int {
	return cmp.Compare(v.val, t.val)
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

type VarcharValue struct {
	t   *Varchar
	val string
}

func (v VarcharValue) Compare(t VarcharValue) int {
	return cmp.Compare(v.val, t.val)
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
	bytes := make([]byte, charCount*4, charCount*4)
	for i, r := range runes {
		binary.LittleEndian.PutUint32(bytes[i*4:], uint32(r))
	}
	return bytes
}

type BooleanValue struct {
	t   *Boolean
	val bool
}

func (v BooleanValue) Compare(t BooleanValue) int {
	if v.val == t.val {
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

type FloatValue struct {
	t   *Float
	val float32
}

func (v FloatValue) Compare(t FloatValue) int {
	return cmp.Compare(v.val, t.val)
}

func (v FloatValue) Type() Type {
	return v.t
}

func (v FloatValue) Val() any {
	return v.val
}

func (v FloatValue) ToBytes() []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, math.Float32bits(v.val))
	return bytes
}
