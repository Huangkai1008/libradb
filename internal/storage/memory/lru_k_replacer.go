package memory

import (
	"container/list"
	"errors"
	"sync"

	"github.com/Huangkai1008/libradb/internal/storage/table"
)

// LRUKReplacer implements the LRU-k replacement policy.
//
// The LRU-k algorithm evicts a buffer page whose backward k-distance
// is maximum of all buffer pages.
// Backward k-distance is computed as the difference in time between
// current timestamp and the timestamp of kth previous access.
//
// A buffer page with less than k historical references is
// given +inf as its backward k-distance.
// When multiple frames have +inf backward k-distance,
// classical LRU algorithm is used to choose victim.
//
// See also: https://en.wikipedia.org/wiki/Page_replacement_algorithm
type LRUKReplacer struct {
	mu sync.Mutex
	k  int
	// size is the number of buffer pages can be evicted.
	size          int
	historyList   *list.List
	historyMap    map[table.PageNumber]*list.Element
	cacheList     *list.List
	cacheMap      map[table.PageNumber]*list.Element
	accessCounter map[table.PageNumber]int
	evictable     map[table.PageNumber]bool
}

func NewLRUKReplacer(k int) *LRUKReplacer {
	return &LRUKReplacer{
		k:             k,
		historyList:   list.New(),
		historyMap:    make(map[table.PageNumber]*list.Element),
		cacheList:     list.New(),
		cacheMap:      make(map[table.PageNumber]*list.Element),
		accessCounter: make(map[table.PageNumber]int),
		evictable:     make(map[table.PageNumber]bool),
	}
}

func (r *LRUKReplacer) Evict() (table.PageNumber, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.size == 0 {
		return table.InvalidPageNumber, ErrNoPageToEvict
	}

	for e := r.historyList.Back(); e != nil; e = e.Prev() {
		pageNumber, ok := e.Value.(table.PageNumber)
		if !ok {
			return table.InvalidPageNumber, errors.New("not a invalid page number")
		}
		if r.evictable[pageNumber] {
			return pageNumber, nil
		}
	}

	for e := r.cacheList.Back(); e != nil; e = e.Prev() {
		pageNumber, ok := e.Value.(table.PageNumber)
		if !ok {
			return table.InvalidPageNumber, errors.New("not a invalid page number")
		}
		if r.evictable[pageNumber] {
			return pageNumber, nil
		}
	}

	return table.InvalidPageNumber, ErrNoPageToEvict
}

func (r *LRUKReplacer) Access(pageNumber table.PageNumber) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.accessCounter[pageNumber]++

	switch {
	case r.accessCounter[pageNumber] == r.k:
		if e, ok := r.historyMap[pageNumber]; ok {
			r.historyList.Remove(e)
			delete(r.historyMap, pageNumber)
		}
		r.cacheList.PushFront(pageNumber)
		r.cacheMap[pageNumber] = r.cacheList.Front()

	case r.accessCounter[pageNumber] > r.k:
		if e, ok := r.cacheMap[pageNumber]; ok {
			r.cacheList.Remove(e)
		}
		r.cacheList.PushFront(pageNumber)
		r.cacheMap[pageNumber] = r.cacheList.Front()

	default:
		if _, ok := r.historyMap[pageNumber]; !ok {
			r.historyList.PushFront(pageNumber)
			r.historyMap[pageNumber] = r.historyList.Front()
		}
	}
}

func (r *LRUKReplacer) SetEvictable(pageNumber table.PageNumber, setEvictable bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.accessCounter[pageNumber] == 0 {
		return
	}

	if !r.evictable[pageNumber] && setEvictable {
		r.size++
	}
	if r.evictable[pageNumber] && !setEvictable {
		r.size--
	}
	r.evictable[pageNumber] = setEvictable
}

func (r *LRUKReplacer) Remove(pageNumber table.PageNumber) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	cnt := r.accessCounter[pageNumber]
	if cnt == 0 {
		return nil
	}

	if !r.evictable[pageNumber] {
		return errors.New("page is not evictable")
	}

	if cnt < r.k {
		if e, ok := r.historyMap[pageNumber]; ok {
			r.historyList.Remove(e)
			delete(r.historyMap, pageNumber)
		}
	} else {
		if e, ok := r.cacheMap[pageNumber]; ok {
			r.cacheList.Remove(e)
			delete(r.cacheMap, pageNumber)
		}
	}
	r.size--
	r.accessCounter[pageNumber] = 0
	r.evictable[pageNumber] = false
	return nil
}
