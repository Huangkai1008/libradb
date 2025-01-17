package memory

import (
	"io"

	"github.com/Huangkai1008/libradb/internal/storage/table"
)

// BufferManager is the interface between the files/index manager and the disk manager.
//
// The buffer manager is responsible for managing pages in memory and
// processing page requests from the file and index manager.
// The buffer manager is responsible for the eviction policy,
// or choosing which pages to evict when space is filled up.
// When pages are evicted from memory or new pages are read in to memory (ApplyNewPage),
// the buffer manager communicates with the disk space manager to perform the required disk operations.
type BufferManager interface {
	// ApplyNewPage reads a page from disk and applies it to memory.
	ApplyNewPage(spaceID table.SpaceID, p table.Page) error
	// FetchPage fetches the specified page.
	FetchPage(pageNumber table.PageNumber, schema *table.Schema) (table.Page, error)
	// Unpin the specified page.
	Unpin(pageNumber table.PageNumber, markDirty bool)
	io.Closer
}
