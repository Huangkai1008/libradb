package bplustree

import (
	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/page/datapage"
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

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

func (tree *BPlusTree) Get(key Key) (*datapage.RecordID, error) {
	leafNode, err := tree.root.Get(key)
	if err != nil {
		return nil, err
	}

	return leafNode.GetRecordID(key), nil
}

func (tree *BPlusTree) Put(key Key, id *datapage.RecordID) error {
	_, err := tree.root.Put(key, id)
	if err != nil {
		return err
	}

	return nil
}

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
