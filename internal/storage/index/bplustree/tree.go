// Package bplustree package is used for indexing.
// Inspired by CS186, UC Berkeley.
//
// See https://cs186berkeley.net/notes/note4/
package bplustree

import (
	"fmt"
	"strings"

	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/table"
	"github.com/Huangkai1008/libradb/internal/util"
	"github.com/Huangkai1008/libradb/pkg/typing"
)

// Metadata of a B+ tree.
//
// Each node (except the root node) must have Order ≤ x ≤ 2 * Order entries assuming no deleting happens
// (it’s possible for leaf nodes to end up with < Order entries if you delete data).
// The entries within each node must be sorted.
type Metadata struct {
	Order        uint16
	Schema       *table.Schema
	tableSpaceID table.SpaceID
	// rootPageNumber cannot be changed.
	rootPageNumber table.PageNumber
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
	defer root.unpin(true)

	tree := &BPlusTree{
		meta:          meta,
		root:          root,
		bufferManager: bufferManager,
	}
	tree.updateRoot(root)

	return tree, nil
}

func (tree *BPlusTree) Get(key Key) (*table.Record, error) {
	leafNode, err := tree.getLeafNode(key)
	if err != nil {
		return nil, err
	}

	record := leafNode.GetRecord(key)
	leafNode.unpin(false)
	return record, nil
}

func (tree *BPlusTree) Put(key Key, record *table.Record) error {
	pair, err := tree.root.Put(key, record)
	if err != nil {
		return err
	}

	if pair == nil {
		return nil
	}

	records := []*table.Record{
		newIndexRecord(pair.Key(), tree.root.PageNumber()),
		newIndexRecord(pair.Key(), pair.Value()),
	}
	root, nodeError := NewInnerNode(
		tree.meta, tree.bufferManager, WithIndexRecords(records),
	)
	if nodeError != nil {
		return nodeError
	}

	root.unpin(true)
	tree.updateRoot(root)
	return nil
}

func (tree *BPlusTree) Delete(key Key) error {
	return tree.root.Delete(key)
}

func (tree *BPlusTree) Scan(key Key) typing.BacktrackingIterator[*table.Record] {
	leftMostLeaf, err := tree.getLeafNode(key)
	if err != nil {
		return nil
	}

	index := util.InsertIndex(key, leftMostLeaf.keys)
	return NewRecordIterator(leftMostLeaf, index)
}

func (tree *BPlusTree) String() string {
	var buffer strings.Builder
	buffer.WriteString("BPlusTree(")
	buffer.WriteString(fmt.Sprintf("root=%v", tree.root))
	buffer.WriteString(")")
	return buffer.String()
}

func (tree *BPlusTree) updateRoot(newRoot BPlusNode) {
	tree.root = newRoot
	tree.meta.rootPageNumber = newRoot.PageNumber()
	tree.meta.incrHeight()
}

func (tree *BPlusTree) getLeafNode(key Key) (*LeafNode, error) {
	return tree.root.Get(key)
}
