package page_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Huangkai1008/libradb/internal/field"
	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

func TestNewDataPage(t *testing.T) {
	t.Run("leaf data page", func(t *testing.T) {
		p := page.NewDataPage(true)

		assert.True(t, p.IsLeaf())
		assert.NotEmpty(t, p.PageNumber())
		assert.Equal(t, page.InvalidPageNumber, p.PrevPageNumber())
		assert.Equal(t, page.InvalidPageNumber, p.NextPageNumber())
		assert.Zero(t, p.RecordCount())
	})

	t.Run("inner data page", func(t *testing.T) {
		p := page.NewDataPage(false)

		assert.False(t, p.IsLeaf())
		assert.NotEmpty(t, p.PageNumber())
		assert.Equal(t, page.InvalidPageNumber, p.PrevPageNumber())
		assert.Equal(t, page.InvalidPageNumber, p.NextPageNumber())
		assert.Zero(t, p.RecordCount())
	})
}

func TestDataPage_Insert(t *testing.T) {
	p := page.NewDataPage(true)

	p.Insert(0, page.NewRecordFromLiteral(1))
	record := p.Get(0)
	assert.EqualValues(t, 1, p.RecordCount())
	assert.True(t, record.Equal(page.NewRecordFromLiteral(1)))

	p.Insert(1, page.NewRecordFromLiteral(2))
	assert.EqualValues(t, 2, p.RecordCount())

	p.Insert(1, page.NewRecordFromLiteral(3))
	record = p.Get(1)
	assert.EqualValues(t, 3, p.RecordCount())
	assert.True(t, record.Equal(page.NewRecordFromLiteral(3)))
	assert.NotEmpty(t, p.String())
}

func TestDataPage_Append(t *testing.T) {
	p := page.NewDataPage(true)

	for i := 0; i < 10; i++ {
		p.Append(page.NewRecordFromLiteral(i))
	}

	assert.EqualValues(t, 10, p.RecordCount())
}

func TestDataPage_Delete(t *testing.T) {
	p := page.NewDataPage(true)
	for i := 1; i < 10; i++ {
		p.Append(page.NewRecordFromLiteral(i))
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
	p := page.NewDataPage(true)
	for i := 0; i < 10; i++ {
		p.Append(page.NewRecordFromLiteral(i))
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
		for _, record := range records {
			p.Append(record)
		}

		buffer := p.Buffer()
		newP := page.DataPageFromBytes(buffer, schema)
		assert.Equal(t, buffer, newP.Buffer())
	})

	p := page.NewDataPage(true)

	buffer := p.Buffer()
	newP := page.DataPageFromBytes(buffer, schema)

	assert.Equal(t, buffer, newP.Buffer())
}
