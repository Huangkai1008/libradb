package bplustree

import (
	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/page/datapage"
	"github.com/Huangkai1008/libradb/internal/util"
)

type InnerNodeOption func(*InnerNode)

// InnerNode is the non-leaf page in the B+ tree.
//
// Inner pages' records contain keys and child page numbers.
type InnerNode struct {
	meta          *Metadata
	page          page.Page
	bufferManager memory.BufferManager

	// keys present the minimum key on the child page they point to,
	// are sorted in ascending Order.
	keys []Key
	// children present the page number of the child page.
	children []page.Number
}

func NewInnerNode(
	meta *Metadata,
	buffManager memory.BufferManager,
	options ...InnerNodeOption,
) (*InnerNode, error) {
	threshold := meta.Order * 2 //nolint:mnd // a threshold is the maximum number of keys in the inner node.
	node := &InnerNode{
		meta:          meta,
		bufferManager: buffManager,
		keys:          make([]Key, 0, threshold),
		children:      make([]page.Number, 0, threshold),
	}
	p, err := buffManager.ApplyNewPage(meta.tableSpaceID)
	if err != nil {
		return nil, err
	}
	node.page = p

	applyInnerNodeOptions(node, options...)
	if err = node.sync(); err != nil {
		return nil, err
	}
	return node, nil
}

func applyInnerNodeOptions(node *InnerNode, options ...InnerNodeOption) {
	for _, option := range options {
		option(node)
	}
}

func WithInnerPage(page page.Page) InnerNodeOption {
	return func(node *InnerNode) {
		node.page = page
	}
}

// Get the leaf node that may contain the key.
func (node *InnerNode) Get(key Key) (*LeafNode, error) {
	index := util.SearchIndex(key, node.keys)
	pageNumber := node.children[index]
	child, err := BPlusNodeFrom(pageNumber, node.meta, node.bufferManager)
	if err != nil {
		return nil, err
	}

	return child.Get(key)
}

func (node *InnerNode) Put(key Key, rid *datapage.RecordID) (*Pair, error) {
	// TODO implement me
	panic("implement me")
}

func (node *InnerNode) sync() error {
	// TODO: implement sync
	return nil
}

func (node *InnerNode) isOverflowed() bool {
	panic("implement me")
}
