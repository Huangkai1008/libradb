package disk

import (
	"sync"

	"github.com/Huangkai1008/libradb/internal/storage/table"
)

type MemoryDiskManager struct {
	mu    sync.Mutex
	pages map[table.PageNumber][]byte
}

func NewMemoryDiskManager() *MemoryDiskManager {
	return &MemoryDiskManager{
		pages: make(map[table.PageNumber][]byte),
	}
}

func (m *MemoryDiskManager) ReadPage(pageNumber table.PageNumber, bytes []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	contents, ok := m.pages[pageNumber]
	if !ok {
		return PageNotAllocated(pageNumber)
	}

	copy(bytes, contents)
	return nil
}

func (m *MemoryDiskManager) WritePage(pageNumber table.PageNumber, bytes []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.pages[pageNumber] = bytes
	return nil
}

func (m *MemoryDiskManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.pages = make(map[table.PageNumber][]byte)
	return nil
}
