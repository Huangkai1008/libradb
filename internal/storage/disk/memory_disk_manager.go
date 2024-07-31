package disk

import (
	"errors"
	"sync"

	"github.com/Huangkai1008/libradb/internal/storage/page"
)

var ErrPageNotAllocated = errors.New("page not allocated")

type MemoryDiskManager struct {
	mu    sync.Mutex
	pages map[page.Number][]byte
}

func NewMemoryDiskManager() *MemoryDiskManager {
	return &MemoryDiskManager{
		pages: make(map[page.Number][]byte),
	}
}

func (m *MemoryDiskManager) ReadPage(pageNumber page.Number, bytes []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	contents, ok := m.pages[pageNumber]
	if !ok {
		return ErrPageNotAllocated
	}

	copy(bytes, contents)
	return nil
}

func (m *MemoryDiskManager) WritePage(pageNumber page.Number, bytes []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.pages[pageNumber] = bytes
	return nil
}
