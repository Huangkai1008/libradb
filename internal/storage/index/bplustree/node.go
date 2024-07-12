package bplustree

import (
	"github.com/Huangkai1008/libradb/internal/field"
	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/page"
)

type Pair struct {
	key   Key
	value page.Number
}

func (p Pair) Key() Key {
	return p.key
}

func (p Pair) Value() page.Number {
	return p.value
}

type Key = field.Value

// BPlusNode represents a page in the B+ tree.
//
// Pages can be either non-leaf (index/internal) nodes or leaf nodes.
// We assign each page in the tree a level in the page header, leaf pages are at level 0,
// and the level increments going up the tree.
type BPlusNode interface {
	// Get the leaf node on which key may reside when queried from node.
	Get(key Key) (*LeafNode, error)
	// Put the key and record identifier into the subtree rooted by node.
	// If put operation causes the node to split,
	// it returns the key and page number of the new node.
	// Otherwise, it returns nil.
	Put(key Key, record *page.Record) (*Pair, error)

	// PageNumber returns the page number of the page underlying the node.
	PageNumber() page.Number

	// sync synchronizes the node with the underlying page.
	sync() error
	// isOverflowed returns true if the node is overflowed.
	isOverflowed() bool
}

// BPlusNodeFrom creates a new B+ tree page.
func BPlusNodeFrom(
	pageNumber page.Number,
	meta *Metadata,
	buffManager memory.BufferManager,
) (BPlusNode, error) {
	p, err := buffManager.FetchPage(meta.tableSpaceID, pageNumber)
	if err != nil {
		return nil, err
	}

	dataPage := page.DataPageFromBytes(p.Buffer())

	if dataPage.IsLeaf() {
		return NewLeafNode(meta, buffManager, WithLeafPage(dataPage))
	}
	return NewInnerNode(meta, buffManager, WithInnerPage(dataPage))
}
