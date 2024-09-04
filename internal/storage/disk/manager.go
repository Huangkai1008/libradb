package disk

import (
	"errors"
	"fmt"
	"io"

	"github.com/Huangkai1008/libradb/internal/storage/table"
)

var ErrPageNotAllocated = errors.New("page not allocated")

func PageNotAllocated(pageNumber table.PageNumber) error {
	return fmt.Errorf("%w: %v", ErrPageNotAllocated, pageNumber)
}

type Manager interface {
	// ReadPage reads a page from disk.
	ReadPage(table.PageNumber, []byte) error
	// WritePage writes a page to disk.
	WritePage(table.PageNumber, []byte) error
	io.Closer
}
