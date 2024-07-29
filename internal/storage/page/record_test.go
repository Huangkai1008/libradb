package page_test

import (
	"fmt"
	"testing"

	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestRecord_Equal(t *testing.T) {
	t.Run("should not equal if value lengths are not match", func(t *testing.T) {
		var tests = []struct {
			record1 *page.Record
			record2 *page.Record
		}{
			{page.NewRecord(), page.NewRecordFromLiteral(1)},
			{page.NewRecord(), page.NewRecordFromLiteral(0, 2)},
			{page.NewRecordFromLiteral(20), page.NewRecordFromLiteral(1, 60)},
			{page.NewRecordFromLiteral(20), page.NewRecordFromLiteral(2, 60)},
		}

		for i, test := range tests {
			t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
				equality := test.record1.Equal(test.record2)

				assert.False(t, equality)
			})
		}
	})

	t.Run("should equal if records values is empty", func(t *testing.T) {
		record1 := page.NewRecord()
		record2 := page.NewRecord()

		equality := record1.Equal(record2)

		assert.True(t, equality)
	})

	t.Run("should equal if records values are equal", func(t *testing.T) {
		var tests = []struct {
			record1 *page.Record
			record2 *page.Record
		}{
			{
				page.NewRecordFromLiteral(1, 60),
				page.NewRecordFromLiteral(1, 60)},
			{
				page.NewRecordFromLiteral(6, "Charlie", 22, true, 80.5),
				page.NewRecordFromLiteral(6, "Charlie", 22, true, 80.5),
			},

			{
				page.NewRecordFromLiteral(7, "Tom", 12, false, 80.5),
				page.NewRecordFromLiteral(7, "Tom", 12, false, 80.5),
			},
		}

		for i, test := range tests {
			t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
				equality := test.record1.Equal(test.record2)

				assert.True(t, equality)
			})
		}
	})
}

func TestRecord_Get(t *testing.T) {
	var tests = []struct {
		record   *page.Record
		index    int
		expected any
	}{
		{page.NewRecordFromLiteral(1, "Hello"), 0, 1},
		{page.NewRecordFromLiteral(1, "Hello"), 1, "Hello"},
		{page.NewRecordFromLiteral(1, "Hello", "Hello1"), 2, "Hello1"},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			assert.EqualValues(t, test.expected, test.record.Get(test.index).Val())
		})
	}
}
func TestRecord_GetKey(t *testing.T) {
	var tests = []struct {
		record   *page.Record
		expected any
	}{
		{page.NewRecordFromLiteral(1, "Hello"), 1},
		{page.NewRecordFromLiteral(2, "Hello"), 2},
		{page.NewRecordFromLiteral(1, "Hello", "Hello1"), 1},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			assert.EqualValues(t, test.expected, test.record.GetKey().Val())
		})
	}
}
