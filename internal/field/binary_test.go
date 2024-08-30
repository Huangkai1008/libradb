package field_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Huangkai1008/libradb/internal/field"
)

func TestBinary_New(t *testing.T) {
	t.Run("should succeed without error", func(t *testing.T) {
		t.Run("default options", func(t *testing.T) {
			assert.NotPanics(t, func() {
				field.NewBinary()
			})
		})

		t.Run("with length option", func(t *testing.T) {
			assert.NotPanics(t,
				func() {
					sut := field.NewBinary(field.WithByteLength(10))
					assert.Equal(t, 10, sut.Length())
				},
			)
		})

		t.Run("with allow null option", func(t *testing.T) {
			assert.NotPanics(t,
				func() {
					sut := field.NewBinary(field.WithAllowNull[*field.Binary](true))
					assert.True(t, sut.AllowNull())
				},
			)
		})
	})

	t.Run("should raise error when length is invalid", func(t *testing.T) {
		var tests = []struct {
			name   string
			length int
		}{
			{"negative", -1},
			{"zero", 0},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.Panics(t, func() {
					field.NewBinary(field.WithByteLength(tt.length))
				})
			})
		}
	})
}

func TestBinary_ByteSize(t *testing.T) {
	t.Run("should be equal to the length of bytes", func(t *testing.T) {
		var tests = []struct {
			name   string
			length int
			result int
		}{
			{"length is 1", 1, 1},
			{"length is 5", 5, 5},
			{"length is 10", 10, 10},
			{"length is 20", 20, 20},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				sut := field.NewBinary(field.WithByteLength(tt.length))

				byteSize := sut.ByteSize()

				assert.Equal(t, tt.result, byteSize)
			})
		}
	})
}
