package table_test

import (
	"fmt"
	"github.com/Huangkai1008/libradb/internal/storage/table"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecord_Equal(t *testing.T) {
	t.Run("should not equal if value lengths are not match", func(t *testing.T) {
		var tests = []struct {
			record1 *table.Record
			record2 *table.Record
		}{
			{table.NewRecord(), table.NewRecordFromLiteral(1)},
			{table.NewRecord(), table.NewRecordFromLiteral(0, 2)},
			{table.NewRecordFromLiteral(20), table.NewRecordFromLiteral(1, 60)},
			{table.NewRecordFromLiteral(20), table.NewRecordFromLiteral(2, 60)},
		}

		for i, test := range tests {
			t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
				equality := test.record1.Equal(test.record2)

				assert.False(t, equality)
			})
		}
	})

	t.Run("should equal if records values is empty", func(t *testing.T) {
		record1 := table.NewRecord()
		record2 := table.NewRecord()

		equality := record1.Equal(record2)

		assert.True(t, equality)
	})

	t.Run("should equal if records values are equal", func(t *testing.T) {
		var tests = []struct {
			record1 *table.Record
			record2 *table.Record
		}{
			{
				table.NewRecordFromLiteral(1, 60),
				table.NewRecordFromLiteral(1, 60)},
			{
				table.NewRecordFromLiteral(6, "Charlie", 22, true, 80.5),
				table.NewRecordFromLiteral(6, "Charlie", 22, true, 80.5),
			},

			{
				table.NewRecordFromLiteral(7, "Tom", 12, false, 80.5),
				table.NewRecordFromLiteral(7, "Tom", 12, false, 80.5),
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
		record   *table.Record
		index    int
		expected any
	}{
		{table.NewRecordFromLiteral(1, "Hello"), 0, 1},
		{table.NewRecordFromLiteral(1, "Hello"), 1, "Hello"},
		{table.NewRecordFromLiteral(1, "Hello", "Hello1"), 2, "Hello1"},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			assert.EqualValues(t, test.expected, test.record.Get(test.index).Val())
		})
	}
}
func TestRecord_GetKey(t *testing.T) {
	var tests = []struct {
		record   *table.Record
		expected any
	}{
		{table.NewRecordFromLiteral(1, "Hello"), 1},
		{table.NewRecordFromLiteral(2, "Hello"), 2},
		{table.NewRecordFromLiteral(1, "Hello", "Hello1"), 1},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			assert.EqualValues(t, test.expected, test.record.GetKey().Val())
		})
	}
}
