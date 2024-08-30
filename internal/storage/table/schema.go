package table

import (
	"github.com/Huangkai1008/libradb/internal/field"
)

const MaxSchemaByteSize = 4096

type fieldName = string

type Schema struct {
	FieldNames []fieldName
	FieldTypes []field.Type
	byteSize   int
}

func NewSchema() *Schema {
	return &Schema{}
}

func (s *Schema) WithField(name fieldName, t field.Type) *Schema {
	s.FieldNames = append(s.FieldNames, name)
	s.FieldTypes = append(s.FieldTypes, t)
	s.byteSize += t.ByteSize()
	return s
}

func (s *Schema) ByteSize() int {
	return s.byteSize
}

// Length returns the number of fields in the schema.
func (s *Schema) Length() int {
	return len(s.FieldNames)
}

// Concat two schema together, returning a new schema
// containing all fields from both schemas.
func (s *Schema) Concat(other *Schema) *Schema {
	newSchema := NewSchema()
	for i := 0; i < s.Length(); i++ {
		newSchema.WithField(s.FieldNames[i], s.FieldTypes[i])
	}
	for i := 0; i < other.Length(); i++ {
		newSchema.WithField(other.FieldNames[i], other.FieldTypes[i])
	}
	return newSchema
}
