package table_test

import (
	"github.com/Huangkai1008/libradb/internal/storage/table"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPageFromBytes(t *testing.T) {
	t.Run("should get a valid data page", func(t *testing.T) {
		p := table.NewDataPage(true)
		s := table.NewSchema()
		contents := p.Buffer()

		newP := table.FromBytes(contents, s)
		assert.Equal(t, contents, newP.Buffer())
	})
}
