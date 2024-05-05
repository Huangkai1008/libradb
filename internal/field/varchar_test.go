package field_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Huangkai1008/libradb/internal/field"
)

func TestVarchar_New(t *testing.T) {
	t.Run("should succeed without error", func(t *testing.T) {
		t.Run("default options", func(t *testing.T) {
			assert.NotPanics(t, func() {
				field.NewVarchar()
			})
		})

		t.Run("with length option", func(t *testing.T) {
			assert.NotPanics(t,
				func() {
					sut := field.NewVarchar(field.WithLength(10))
					assert.Equal(t, 10, sut.Length())
				},
			)
		})

		t.Run("with allow null option", func(t *testing.T) {
			assert.NotPanics(t,
				func() {
					sut := field.NewVarchar(field.WithAllowNull[*field.Varchar](true))
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
					field.NewVarchar(field.WithLength(tt.length))
				})
			})
		}
	})
}

func TestVarchar_ByteSize(t *testing.T) {
	t.Run("should be multiplied by length and 4", func(t *testing.T) {
		var tests = []struct {
			name   string
			length int
			result int
		}{
			{"length is 1", 1, 4},
			{"length is 5", 5, 20},
			{"length is 10", 10, 40},
			{"length is 20", 20, 80},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				sut := field.NewVarchar(field.WithLength(tt.length))

				bytesize := sut.ByteSize()

				assert.Equal(t, tt.result, bytesize)
			})
		}
	})
}
