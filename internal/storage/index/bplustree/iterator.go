package bplustree

import (
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

type RecordIterator struct {
	cur *LeafNode
	pos int
}

func NewRecordIterator(head *LeafNode, startPos int) *RecordIterator {
	return &RecordIterator{
		cur: head,
		pos: startPos,
	}
}

func (it *RecordIterator) Prev() *table.Record {
	var record *table.Record

	defer func() {
		if record != nil {
			it.pos--
		}
	}()

	if it.pos >= 0 {
		if it.pos == len(it.cur.records()) {
			it.pos--
		}
		record = it.cur.records()[it.pos]
		return record
	}

	// Move to prev leaf page.
	prevPageNumber := it.cur.page.PrevPageNumber()
	if prevPageNumber == table.InvalidPageNumber {
		return nil
	}

	leaf, err := BPlusNodeFrom(prevPageNumber, it.cur.meta, it.cur.bufferManager)
	if err != nil {
		return nil
	}

	cur, ok := leaf.(*LeafNode)
	if !ok {
		return nil
	}

	it.cur, it.pos = cur, len(cur.records())-1
	record = it.cur.records()[it.pos]
	return record
}

func (it *RecordIterator) Next() *table.Record {
	var record *table.Record

	defer func() {
		if record != nil {
			it.pos++
		}
	}()

	if it.pos < len(it.cur.records()) {
		record = it.cur.records()[it.pos]
		return record
	}

	// Move to next leaf page.
	nextPageNumber := it.cur.page.NextPageNumber()
	if nextPageNumber == table.InvalidPageNumber {
		return nil
	}

	leaf, err := BPlusNodeFrom(nextPageNumber, it.cur.meta, it.cur.bufferManager)
	if err != nil {
		return nil
	}

	cur, ok := leaf.(*LeafNode)
	if !ok {
		return nil
	}

	it.cur, it.pos = cur, 0
	record = it.cur.records()[it.pos]
	return record
}
