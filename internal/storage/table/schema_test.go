package table_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Huangkai1008/libradb/internal/field"
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

func TestSchema_Length(t *testing.T) {
	t.Run("should be 0 when schema is empty", func(t *testing.T) {
		s := table.NewSchema()
		assert.Equal(t, 0, s.Length())
	})

	t.Run("should be the number of fields", func(t *testing.T) {
		var tests = []struct {
			name     string
			s        *table.Schema
			expected int
		}{
			{"one field", table.NewSchema().
				WithField("x", field.NewInteger()),
				1},
			{"two fields", table.NewSchema().
				WithField("x", field.NewInteger()).
				WithField("y", field.NewInteger()),
				2},
			{"three fields",
				table.NewSchema().
					WithField("x", field.NewInteger()).
					WithField("y", field.NewInteger()).
					WithField("z", field.NewInteger()),
				3},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.Equal(t, tt.expected, tt.s.Length())
			})
		}
	})
}

func TestSchema_ByteSize(t *testing.T) {
	t.Run("should be 0 when schema is empty", func(t *testing.T) {
		s := table.NewSchema()
		assert.Equal(t, 0, s.ByteSize())
	})

	t.Run("should be the sum of all fields' byte size", func(t *testing.T) {
		t.Run("one field", func(t *testing.T) {
			var tests = []struct {
				name     string
				s        *table.Schema
				expected int
			}{
				{"integer", table.NewSchema().WithField("x", field.NewInteger()), 4},
				{"varchar", table.NewSchema().WithField("x", field.NewVarchar(field.WithLength(10))), 40},
				{"boolean", table.NewSchema().WithField("x", field.NewBoolean()), 1},
				{"float", table.NewSchema().WithField("x", field.NewFloat()), 4},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					assert.Equal(t, tt.expected, tt.s.ByteSize())
				})
			}
		})

		t.Run("multiple fields", func(t *testing.T) {
			s := table.NewSchema().
				WithField("x", field.NewInteger()).
				WithField("y", field.NewVarchar(field.WithLength(10))).
				WithField("z", field.NewBoolean())

			assert.Equal(t, 4+40+1, s.ByteSize())
		})
	})
}
