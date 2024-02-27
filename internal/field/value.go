package field

import (
	"encoding/binary"
	"errors"
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

type IntegerValue struct {
	t   *Integer
	val int32
}

func NewIntegerValue(t *Integer, val int32) (IntegerValue, error) {
	v := IntegerValue{t: t, val: val}
	if err := validate(v); err != nil {
		return IntegerValue{}, err
	}
	return v, nil
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

func NewVarcharValue(t *Varchar, val string) (VarcharValue, error) {
	v := VarcharValue{t: t, val: val}
	if err := validate(v); err != nil {
		return VarcharValue{}, err
	}
	return v, nil
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

func IsNull(v Value) bool {
	return v.Val() == nil
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
	}
	return nil, ErrValueNil
}

func validate(v Value) error {
	if !v.Type().AllowNull() && IsNull(v) {
		return ErrValueNil
	}
	return nil
}
