package bplustree

import (
	"github.com/Huangkai1008/libradb/internal/storage/page"
)

type RecordIterator struct {
	cur *LeafNode
	pos int
}

func (it *RecordIterator) Prev() (record *page.Record) {
	defer func() {
		if record != nil {
			it.pos--
		}
	}()

	if it.pos >= 0 {
		return it.cur.records()[it.pos]
	}

	// Move to prev leaf page.
	prevPageNumber := it.cur.page.NextPageNumber()
	if prevPageNumber == page.InvalidPageNumber {
		return nil
	}

	leaf, err := BPlusNodeFrom(prevPageNumber, it.cur.meta, it.cur.bufferManager)
	if err != nil {
		return nil
	}

	it.cur = leaf.(*LeafNode)
	it.pos = len(it.cur.records()) - 1
	return it.cur.records()[it.pos]
}

func (it *RecordIterator) Next() (record *page.Record) {
	defer func() {
		if record != nil {
			it.pos++
		}
	}()

	if it.pos < len(it.cur.records()) {
		return it.cur.records()[it.pos]
	}

	// Move to next leaf page.
	nextPageNumber := it.cur.page.NextPageNumber()
	if nextPageNumber == page.InvalidPageNumber {
		return nil
	}

	leaf, err := BPlusNodeFrom(nextPageNumber, it.cur.meta, it.cur.bufferManager)
	if err != nil {
		return nil
	}

	it.cur = leaf.(*LeafNode)
	it.pos = 0
	return it.cur.records()[it.pos]
}
