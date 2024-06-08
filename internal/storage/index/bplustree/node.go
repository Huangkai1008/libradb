package bplustree

import (
	"github.com/Huangkai1008/libradb/internal/field"
	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/page/datapage"
)

type Pair struct {
	Key   field.Value
	value page.Number
}

type Key = field.Value

// BPlusNode represents a page in the B+ tree.
//
// Pages can be either non-leaf (inner/internal) nodes or leaf nodes.
// We assign each page in the tree a level in the page header, leaf pages are at level 0,
// and the level increments going up the tree.
type BPlusNode interface {
	// Get the leaf node on which key may reside when queried from node.
	Get(key Key) (*LeafNode, error)
	// Put the key and record identifier into the subtree rooted by node.
	// If put operation causes the node to split,
	// it returns the key and page number of the new node.
	// Otherwise, it returns nil.
	Put(key Key, rid *datapage.RecordID) (*Pair, error)

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

	if p.IsLeaf() {
		return NewLeafNode(meta, buffManager, WithLeafPage(p))
	}
	return NewInnerNode(meta, buffManager, WithInnerPage(p))
}
