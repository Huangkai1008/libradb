// Package bplustree package is used for indexing.
// Inspired by CS186, UC Berkeley.
//
// See https://cs186berkeley.net/notes/note4/
package bplustree

import (
	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

// Metadata of a B+ tree.
//
// Each node (except the root node) must have Order ≤ x ≤ 2 * Order entries assuming no deleting happens
// (it’s possible for leaf nodes to end up with < Order entries if you delete data).
// The entries within each node must be sorted.
type Metadata struct {
	Order        uint32
	tableSpaceID table.SpaceID
	// rootPageNumber cannot be changed.
	rootPageNumber page.Number
	height         uint32
}

func (meta *Metadata) incrHeight() {
	meta.height++
}

// BPlusTree is used for indexing.
//
// An index tree starts at a root page and has a height.
// Different from InnoDB, the root page can be updated.
type BPlusTree struct {
	meta *Metadata
	root BPlusNode

	bufferManager memory.BufferManager
}

func NewBPlusTree(meta *Metadata, bufferManager memory.BufferManager) (*BPlusTree, error) {
	root, err := NewLeafNode(meta, bufferManager)
	if err != nil {
		return nil, err
	}

	meta.incrHeight()
	meta.rootPageNumber = root.page.PageNumber()

	tree := &BPlusTree{
		meta:          meta,
		root:          root,
		bufferManager: bufferManager,
	}

	return tree, nil
}

func (tree *BPlusTree) Get(key Key) (*page.Record, error) {
	leafNode, err := tree.root.Get(key)
	if err != nil {
		return nil, err
	}

	return leafNode.GetRecord(key), nil
}

func (tree *BPlusTree) Put(key Key, record *page.Record) error {
	pair, err := tree.root.Put(key, record)
	if err != nil {
		return err
	}

	if pair == nil {
		return nil
	}

	keys := []Key{pair.Key()}
	children := []page.Number{tree.root.PageNumber(), pair.Value()}
	root, err1 := NewInnerNode(
		tree.meta, tree.bufferManager, WithInnerKeys(keys), WithChildren(children),
	)
	if err1 != nil {
		return err1
	}
	return tree.updateRoot(root)
}

func (tree *BPlusTree) updateRoot(newRoot BPlusNode) error {
	tree.root = newRoot
	tree.meta.rootPageNumber = newRoot.PageNumber()
	tree.meta.incrHeight()
	return nil
}
