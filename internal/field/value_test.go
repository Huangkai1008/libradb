package field

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntegerValue_ToBytes(t *testing.T) {
	typ := NewInteger()
	var tests = []struct {
		val int32
	}{
		{0},
		{-1},
		{42},
		{100},
		{1000},
		{-1000},
		{math.MaxInt32},
		{math.MinInt32},
	}
	for _, tt := range tests {
		v, _ := NewIntegerValue(typ, tt.val)

		bytes := v.ToBytes()
		newV, err := FromBytes(typ, bytes)

		assert.Equal(t, 4, len(bytes))
		assert.NoError(t, err)
		assert.Equal(t, v.Val(), newV.Val())
	}
}

func TestVarcharValue_ToBytes(t *testing.T) {
	typ := NewVarchar()
	var tests = []struct {
		val    string
		length int
	}{
		{"", 0},
		{"a", 1},
		{"ab", 2},
		{"好的", 2},
		{"hello world", 11},
		{"你好，世界", 5},
	}
	for _, tt := range tests {
		t.Run(tt.val, func(t *testing.T) {
			v, _ := NewVarcharValue(typ, tt.val)

			bytes := v.ToBytes()
			newV, err := FromBytes(typ, bytes)

			assert.Equal(t, Bytesize(v), len(bytes))
			assert.NoError(t, err)
			assert.Equal(t, v.Val(), newV.Val())
		})
	}
}
