package page_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Huangkai1008/libradb/internal/field"
	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

func TestDataPage_Buffer(t *testing.T) {
	schema := table.NewSchema().
		WithField("id", field.NewInteger()).
		WithField("name", field.NewVarchar()).
		WithField("age", field.NewInteger()).
		WithField("is_student", field.NewBoolean()).
		WithField("score", field.NewFloat())

	t.Run("no records", func(t *testing.T) {
		p := page.NewDataPage(true)

		buffer := p.Buffer()
		newP := page.DataPageFromBytes(buffer, schema)
		assert.Equal(t, buffer, newP.Buffer())
	})

	t.Run("with records", func(t *testing.T) {
		p := page.NewDataPage(true)
		records := []*page.Record{
			page.NewRecordFromLiteral(4, "Alice", 20, true, 90.5),
			page.NewRecordFromLiteral(9, "Bob", 21, false, 85.5),
			page.NewRecordFromLiteral(6, "Charlie", 22, true, 80.5),
		}
		p.SetRecords(records)

		buffer := p.Buffer()
		newP := page.DataPageFromBytes(buffer, schema)
		assert.Equal(t, buffer, newP.Buffer())
	})

	p := page.NewDataPage(true)

	buffer := p.Buffer()
	newP := page.DataPageFromBytes(buffer, schema)

	assert.Equal(t, buffer, newP.Buffer())
}
