package disk

import (
	"errors"
	"fmt"

	"github.com/Huangkai1008/libradb/internal/storage/page"
)

var ErrPageNotAllocated = errors.New("page not allocated")

func PageNotAllocated(pageNumber page.Number) error {
	return fmt.Errorf("%w: %v", ErrPageNotAllocated, pageNumber)
}

type SpaceManager interface {
	// ReadPage reads a page from disk.
	ReadPage(page.Number, []byte) error
	// WritePage writes a page to disk.
	WritePage(page.Number, []byte) error
}
