package memory

import (
	"errors"
	"sync"

	"github.com/Huangkai1008/libradb/internal/config"
	"github.com/Huangkai1008/libradb/internal/storage/disk"
	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/table"
	"github.com/Huangkai1008/libradb/pkg/ds"
)

var (
	ErrBufferPoolIsFull = errors.New("buffer pool is full")
)

type controlBlock struct {
	// bufferPage holds the pointer to the buffer page.
	bufferPage page.Page
}

type BufferPool struct {
	mu          sync.RWMutex
	diskManager disk.SpaceManager
	// poolSize is the size of the buffer pool.
	poolSize uint16

	// freeLinkedList is a linked list of free control blocks.
	freeLinkedList ds.LinkedList[*controlBlock]
	// flushLinkedList is a chan of control blocks that need to be flushed.
	flushCh chan *controlBlock
	// pageTable is a map of page number to control block.
	pageTable map[page.Number]*controlBlock
	// pinCounter hold the pin/reference count of every page.
	pinCounter map[page.Number]int
	// spaceTable is the map to hold pageNumber to table space ID.
	spaceTable map[page.Number]table.SpaceID
	// replacer is the page eviction policy.
	replacer Replacer
}

func NewBufferPool(
	poolSize uint16,
	diskManager disk.SpaceManager,
	replacer Replacer,
) *BufferPool {
	m := &BufferPool{
		diskManager:    diskManager,
		poolSize:       poolSize,
		freeLinkedList: ds.NewDLL[*controlBlock](),
		flushCh:        make(chan *controlBlock, poolSize),
		replacer:       replacer,
		pageTable:      make(map[page.Number]*controlBlock),
		spaceTable:     make(map[page.Number]table.SpaceID),
		pinCounter:     make(map[page.Number]int),
	}

	for i := uint16(0); i < poolSize; i++ {
		m.freeLinkedList.Append(&controlBlock{
			bufferPage: nil,
		})
	}

	go m.flushPages()
	return m
}

// ApplyNewPage create a new page in the buffer pool.
func (m *BufferPool) ApplyNewPage(spaceID table.SpaceID, p page.Page) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var cb *controlBlock
	pageNumber := p.PageNumber()
	m.spaceTable[pageNumber] = spaceID

	// Always find page space from the free linked list first.
	if m.isFree() {
		cb = m.freeLinkedList.Remove(0)
		cb.bufferPage = p
	} else {
		if err := m.evictPage(); err != nil {
			return err
		}
		cb = &controlBlock{bufferPage: p}
	}
	m.pageTable[pageNumber] = cb
	m.pin(pageNumber)
	return nil
}

func (m *BufferPool) FetchPage(pageNumber page.Number, s *table.Schema) (page.Page, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If the page is already in the buffer pool, return it.
	if cb, ok := m.pageTable[pageNumber]; ok {
		bufferPage := cb.bufferPage
		m.pin(pageNumber)
		return bufferPage, nil
	}

	var cb *controlBlock
	// Always find page space from the free linked list first.
	if m.isFree() {
		cb = m.freeLinkedList.Remove(0)
	} else {
		if err := m.evictPage(); err != nil {
			return nil, err
		}
		cb = &controlBlock{}
	}

	// If the page does not exist in the buffer pool, fetch it from the disk.
	pageContent := make([]byte, config.PageSize)
	if err := m.diskManager.ReadPage(pageNumber, pageContent); err != nil {
		return nil, err
	}

	p := page.FromBytes(pageContent, s)
	cb.bufferPage = p
	m.pin(pageNumber)
	return p, nil
}

func (m *BufferPool) pin(pageNumber page.Number) {
	m.pinCounter[pageNumber]++
	m.replacer.Access(pageNumber)
	m.replacer.SetEvictable(pageNumber, false)
}

func (m *BufferPool) Unpin(pageNumber page.Number, markDirty bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isPinned(pageNumber) {
		return
	}

	m.pinCounter[pageNumber]--
	if m.pinCounter[pageNumber] == 0 {
		m.replacer.SetEvictable(pageNumber, true)
	}

	if markDirty {
		m.flushCh <- m.pageTable[pageNumber]
	}
}

func (m *BufferPool) Close() error {
	close(m.flushCh)
	return nil
}

func (m *BufferPool) isPinned(pageNumber page.Number) bool {
	return m.pinCounter[pageNumber] > 0
}

func (m *BufferPool) isFree() bool {
	return m.freeLinkedList.Size() > 0
}

func (m *BufferPool) evictPage() error {
	// Choose page to evict.
	evictedNumber, err := m.replacer.Evict()
	if errors.Is(err, ErrNoPageToEvict) {
		return ErrBufferPoolIsFull
	}
	if err != nil {
		return err
	}
	// remove page from replacer and buffer pool
	if err = m.replacer.Remove(evictedNumber); err != nil {
		return err
	}

	if err = m.flushPage(evictedNumber); err != nil {
		return err
	}
	delete(m.pageTable, evictedNumber)
	return nil
}

func (m *BufferPool) flushPage(pageNumber page.Number) error {
	cb, ok := m.pageTable[pageNumber]
	if ok {
		return m.diskManager.WritePage(pageNumber, cb.bufferPage.Buffer())
	}
	return nil
}

func (m *BufferPool) flushPages() {
	for cb := range m.flushCh {
		p := cb.bufferPage
		contents := p.Buffer()
		_ = m.diskManager.WritePage(p.PageNumber(), contents)
	}
}
