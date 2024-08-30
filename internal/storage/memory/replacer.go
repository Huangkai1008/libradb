package memory

import (
	"errors"
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

var ErrNoPageToEvict = errors.New("no page to evict")

// Replacer is the interface to track page usage.
type Replacer interface {
	Evict() (table.PageNumber, error)
	Access(pageNumber table.PageNumber)
	SetEvictable(pageNumber table.PageNumber, evictable bool)
	Remove(pageNumber table.PageNumber) error
}
