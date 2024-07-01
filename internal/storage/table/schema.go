package table

import "github.com/Huangkai1008/libradb/internal/field"

type fieldName = string

type Schema struct {
	fieldNames []fieldName
	FieldTypes []field.Type
	bytesize   int
}

func NewSchema() *Schema {
	return &Schema{}
}

func (s *Schema) WithField(name fieldName, t field.Type) *Schema {
	s.fieldNames = append(s.fieldNames, name)
	s.FieldTypes = append(s.FieldTypes, t)
	s.bytesize += t.ByteSize()
	return s
}

func (s *Schema) ByteSize() int {
	return s.bytesize
}

func (s *Schema) Length() int {
	return len(s.fieldNames)
}
