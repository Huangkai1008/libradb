package table_test

import (
	"github.com/Huangkai1008/libradb/internal/storage/table"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Huangkai1008/libradb/internal/field"
)

func TestNewDataPage(t *testing.T) {
	t.Run("leaf data page", func(t *testing.T) {
		p := table.NewDataPage(true)

		assert.True(t, p.IsLeaf())
		assert.NotEmpty(t, p.PageNumber())
		assert.Equal(t, table.InvalidPageNumber, p.PrevPageNumber())
		assert.Equal(t, table.InvalidPageNumber, p.NextPageNumber())
		assert.Zero(t, p.RecordCount())
	})

	t.Run("inner data page", func(t *testing.T) {
		p := table.NewDataPage(false)

		assert.False(t, p.IsLeaf())
		assert.NotEmpty(t, p.PageNumber())
		assert.Equal(t, table.InvalidPageNumber, p.PrevPageNumber())
		assert.Equal(t, table.InvalidPageNumber, p.NextPageNumber())
		assert.Zero(t, p.RecordCount())
	})
}

func TestDataPage_Insert(t *testing.T) {
	p := table.NewDataPage(true)

	p.Insert(0, table.NewRecordFromLiteral(1))
	record := p.Get(0)
	assert.EqualValues(t, 1, p.RecordCount())
	assert.True(t, record.Equal(table.NewRecordFromLiteral(1)))

	p.Insert(1, table.NewRecordFromLiteral(2))
	assert.EqualValues(t, 2, p.RecordCount())

	p.Insert(1, table.NewRecordFromLiteral(3))
	record = p.Get(1)
	assert.EqualValues(t, 3, p.RecordCount())
	assert.True(t, record.Equal(table.NewRecordFromLiteral(3)))
	assert.NotEmpty(t, p.String())
}

func TestDataPage_Append(t *testing.T) {
	p := table.NewDataPage(true)

	for i := 0; i < 10; i++ {
		p.Append(table.NewRecordFromLiteral(i))
	}

	assert.EqualValues(t, 10, p.RecordCount())
}

func TestDataPage_Delete(t *testing.T) {
	p := table.NewDataPage(true)
	for i := 1; i < 10; i++ {
		p.Append(table.NewRecordFromLiteral(i))
	}

	record := p.Get(5)
	removed := p.Delete(5)
	assert.True(t, removed.Equal(record))

	for i := 1; i < 9; i++ {
		p.Delete(0)
	}
	assert.EqualValues(t, 0, p.RecordCount())
}

func TestDataPage_Shrink(t *testing.T) {
	p := table.NewDataPage(true)
	for i := 0; i < 10; i++ {
		p.Append(table.NewRecordFromLiteral(i))
	}
	assert.EqualValues(t, 10, p.RecordCount())

	removed := p.Shrink(4)

	assert.Len(t, removed, 6)
	assert.EqualValues(t, 4, p.RecordCount())
}

func TestDataPage_Buffer(t *testing.T) {
	schema := table.NewSchema().
		WithField("id", field.NewInteger()).
		WithField("name", field.NewVarchar()).
		WithField("age", field.NewInteger()).
		WithField("is_student", field.NewBoolean()).
		WithField("score", field.NewFloat())

	t.Run("no records", func(t *testing.T) {
		p := table.NewDataPage(true)

		buffer := p.Buffer()
		newP := table.DataPageFromBytes(buffer, schema)
		assert.Equal(t, buffer, newP.Buffer())
	})

	t.Run("with records", func(t *testing.T) {
		p := table.NewDataPage(true)
		records := []*table.Record{
			table.NewRecordFromLiteral(4, "Alice", 20, true, 90.5),
			table.NewRecordFromLiteral(9, "Bob", 21, false, 85.5),
			table.NewRecordFromLiteral(6, "Charlie", 22, true, 80.5),
		}
		for _, record := range records {
			p.Append(record)
		}

		buffer := p.Buffer()
		newP := table.DataPageFromBytes(buffer, schema)
		assert.Equal(t, buffer, newP.Buffer())
	})

	p := table.NewDataPage(true)

	buffer := p.Buffer()
	newP := table.DataPageFromBytes(buffer, schema)

	assert.Equal(t, buffer, newP.Buffer())
}
