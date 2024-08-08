package field_test

import (
	"testing"

	"github.com/Huangkai1008/libradb/internal/field"
	"github.com/stretchr/testify/assert"
)

func TestNewBoolean(t *testing.T) {
	t.Run("should succeed without error", func(t *testing.T) {
		t.Run("default options", func(t *testing.T) {
			assert.NotPanics(t, func() {
				field.NewBoolean()
			})
		})

		t.Run("with allow null option", func(t *testing.T) {
			assert.NotPanics(t,
				func() {
					sut := field.NewBoolean(field.WithAllowNull[*field.Boolean](true))
					assert.True(t, sut.AllowNull())
				},
			)
		})
	})
}
