package page_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

func TestPageFromBytes(t *testing.T) {
	t.Run("should get a valid data page", func(t *testing.T) {
		p := page.NewDataPage(true)
		s := table.NewSchema()
		contents := p.Buffer()

		newP := page.FromBytes(contents, s)
		assert.Equal(t, contents, newP.Buffer())
	})
}
