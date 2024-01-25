package field

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVarchar_New(t *testing.T) {
	t.Run("should succeed without error", func(t *testing.T) {
		t.Run("default options", func(t *testing.T) {
			assert.NotPanics(t, func() {
				NewVarchar()
			})
		})

		t.Run("with length option", func(t *testing.T) {
			assert.NotPanics(t,
				func() {
					sut := NewVarchar(WithLength(10))
					assert.Equal(t, sut.length, 10)
				},
			)
		})

		t.Run("with allow null option", func(t *testing.T) {
			assert.NotPanics(t,
				func() {
					sut := NewVarchar(WithAllowNull[*Varchar](true))
					assert.Equal(t, sut.AllowNull(), true)
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
					NewVarchar(WithLength(tt.length))
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
				sut := NewVarchar(WithLength(tt.length))

				bytesize := sut.ByteSize()

				assert.Equal(t, tt.result, bytesize)
			})
		}
	})
}
