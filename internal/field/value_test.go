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
		val int
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

func TestIntegerValue_Compare(t *testing.T) {
	typ := field.NewInteger()
	var tests = []struct {
		val1     int
		val2     int
		expected bool
	}{
		{0, 1, false},
		{-1, 1, false},
		{0, 0, true},
		{math.MinInt32, math.MinInt32, true},
		{1000, 1000, true},
	}

	for _, tt := range tests {
		v1 := field.NewValue(typ, tt.val1)
		v2 := field.NewValue(typ, tt.val2)

		equality := v1.Compare(v2) == 0

		assert.Equal(t, tt.expected, equality)
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
		{"David", 5},
	}
	for _, tt := range tests {
		t.Run(tt.val, func(t *testing.T) {
			v := field.NewValue(typ, tt.val)

			bytes := v.ToBytes()
			newV, err := field.FromBytes(typ, bytes)

			require.NoError(t, err)
			assert.Len(t, bytes, field.ByteSize(v))
			assert.Equal(t, v.Val(), newV.Val())
		})
	}
}

func TestVarcharValue_Compare(t *testing.T) {
	typ := field.NewVarchar()
	var tests = []struct {
		val1     string
		val2     string
		expected bool
	}{
		{"Hello World", "Hello World", true},
		{"Hello World1", "Hello World", false},
		{"", "", true},
		{"a", "a", true},
		{"ab", "ab", true},
	}
	for _, tt := range tests {
		v1 := field.NewValue(typ, tt.val1)
		v2 := field.NewValue(typ, tt.val2)

		equality := v1.Compare(v2) == 0

		assert.Equal(t, tt.expected, equality)
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

func TestBooleanValue_Compare(t *testing.T) {
	typ := field.NewBoolean()
	var tests = []struct {
		val1     bool
		val2     bool
		expected bool
	}{
		{true, true, true},
		{false, true, false},
		{true, false, false},
		{false, true, false},
		{false, false, true},
	}

	for _, tt := range tests {
		v1 := field.NewValue(typ, tt.val1)
		v2 := field.NewValue(typ, tt.val2)

		equality := v1.Compare(v2) == 0

		assert.Equal(t, tt.expected, equality)
	}
}

func TestFloatValue_ToBytes(t *testing.T) {
	typ := field.NewFloat()
	var tests = []struct {
		val float64
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

func TestFloatValue_Compare(t *testing.T) {
	typ := field.NewFloat()
	var tests = []struct {
		val1     float64
		val2     float64
		expected bool
	}{
		{0.0, 1.0, false},
		{-1.0, 1, false},
		{0.0, 0, true},
		{0.0, 1.0, false},
		{-1.0, 1.0, false},
	}

	for _, tt := range tests {
		v1 := field.NewValue(typ, tt.val1)
		v2 := field.NewValue(typ, tt.val2)

		equality := v1.Compare(v2) == 0

		assert.Equal(t, tt.expected, equality)
	}
}
