package disk

import "github.com/Huangkai1008/libradb/internal/storage/page"

type SpaceManager interface {
	// ReadPage reads a page from disk.
	ReadPage(page.Number, []byte) error
	// WritePage writes a page to disk.
	WritePage(page.Number, []byte) error
}
