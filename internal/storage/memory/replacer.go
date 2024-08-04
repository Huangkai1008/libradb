package memory

import (
	"errors"

	"github.com/Huangkai1008/libradb/internal/storage/page"
)

var ErrNoPageToEvict = errors.New("no page to evict")

// Replacer is the interface to track page usage.
type Replacer interface {
	Evict() (page.Number, error)
	Access(pageNumber page.Number)
	SetEvictable(pageNumber page.Number, evictable bool)
	Remove(pageNumber page.Number) error
}
