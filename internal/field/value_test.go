package field_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Huangkai1008/libradb/internal/field"
)

func TestIntegerValue_ToBytes(t *testing.T) {
	typ := field.NewInteger()
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
		v := field.NewValue(typ, tt.val)

		bytes := v.ToBytes()
		newV, err := field.FromBytes(typ, bytes)

		require.NoError(t, err)
		assert.Len(t, bytes, 4)
		assert.Equal(t, v.Val(), newV.Val())
	}
}

func TestVarcharValue_ToBytes(t *testing.T) {
	typ := field.NewVarchar()
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
			v := field.NewValue(typ, tt.val)

			bytes := v.ToBytes()
			newV, err := field.FromBytes(typ, bytes)

			require.NoError(t, err)
			assert.Len(t, bytes, field.Bytesize(v))
			assert.Equal(t, v.Val(), newV.Val())
		})
	}
}

func TestBooleanValue_ToBytes(t *testing.T) {
	typ := field.NewBoolean()
	var tests = []struct {
		name string
		val  bool
	}{
		{"true", true},
		{"false", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := field.NewValue(typ, tt.val)

			bytes := v.ToBytes()
			newV, err := field.FromBytes(typ, bytes)

			require.NoError(t, err)
			assert.Len(t, bytes, 1)
			assert.Equal(t, v.Val(), newV.Val())
		})
	}
}

func TestFloatValue_ToBytes(t *testing.T) {
	typ := field.NewFloat()
	var tests = []struct {
		val float32
	}{
		{0},
		{-1.23},
		{42.213},
		{100.231},
		{1000.112},
		{-1000.23341},
		{math.MaxFloat32},
		{math.SmallestNonzeroFloat32},
	}
	for _, tt := range tests {
		v := field.NewValue(typ, tt.val)

		bytes := v.ToBytes()
		newV, err := field.FromBytes(typ, bytes)

		require.NoError(t, err)
		assert.Len(t, bytes, 4)
		assert.Equal(t, v.Val(), newV.Val())
	}
}
