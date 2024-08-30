package bplustree

import (
	"errors"
	"github.com/Huangkai1008/libradb/internal/storage/table"

	"github.com/Huangkai1008/libradb/internal/field"
	"github.com/Huangkai1008/libradb/internal/storage/memory"
)

type Pair struct {
	key   Key
	value table.PageNumber
}

func (p Pair) Key() Key {
	return p.key
}

func (p Pair) Value() table.PageNumber {
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
	Put(key Key, record *table.Record) (*Pair, error)
	// Delete the key and its corresponding record from the subtree rooted by node,
	// or does nothing if the key is not in the subtree.
	// Note, delete not re-balance the tree, delete the key and record simply.
	Delete(key Key) error
	// PageNumber returns the page number of the page underlying the node.
	PageNumber() table.PageNumber

	// isOverflowed returns true if the node is overflowed.
	isOverflowed() bool
	// unpin buffer page.
	unpin(markDirty bool)
}

// BPlusNodeFrom creates a new B+ tree page.
func BPlusNodeFrom(
	pageNumber table.PageNumber,
	meta *Metadata,
	buffManager memory.BufferManager,
) (BPlusNode, error) {
	p, err := buffManager.FetchPage(pageNumber, meta.Schema)
	if err != nil {
		return nil, err
	}

	dataPage, ok := p.(*table.DataPage)
	if !ok {
		return nil, errors.New("not a data page")
	}

	if dataPage.IsLeaf() {
		return leafNodeFromPage(meta, buffManager, dataPage), nil
	}
	return innerNodeFromPage(meta, buffManager, dataPage), nil
}

func newIndexRecord(key Key, pageNumber table.PageNumber) *table.Record {
	return table.NewRecord(key, field.NewValue(field.NewInteger(), int(pageNumber)))
}
