package bplustree

import (
	"errors"
	"slices"

	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/page/datapage"
	"github.com/Huangkai1008/libradb/internal/util"
)

var (
	// ErrKeyExists is returned when the key already exists in the leaf node.
	ErrKeyExists = errors.New("key already exists")
)

type LeafNodeOption func(*LeafNode)

func applyLeafNodeOptions(node *LeafNode, options ...LeafNodeOption) {
	for _, option := range options {
		option(node)
	}
}

// LeafNode is the leaf page in the B+ tree.
type LeafNode struct {
	meta          *Metadata
	page          page.Page
	bufferManager memory.BufferManager

	// keys present the primary key of the record.
	keys []Key
	// rids present the record identifier of the record.
	rids []*datapage.RecordID
	// prevPageNumber and nextPageNumber are the sibling leaf nodes.
	prevPageNumber page.Number
	nextPageNumber page.Number
}

func NewLeafNode(
	meta *Metadata,
	buffManager memory.BufferManager,
	options ...LeafNodeOption,
) (*LeafNode, error) {
	threshold := meta.Order * 2 //nolint:mnd // a threshold is the maximum number of keys in the leaf node.
	node := &LeafNode{
		meta:          meta,
		bufferManager: buffManager,
		keys:          make([]Key, 0, threshold),
		rids:          make([]*datapage.RecordID, 0, threshold),
	}
	p, err := buffManager.ApplyNewPage(meta.tableSpaceID)
	if err != nil {
		return nil, err
	}
	node.page = p

	applyLeafNodeOptions(node, options...)
	if err = node.sync(); err != nil {
		return nil, err
	}

	return node, nil
}

func WithKeys(keys []Key) LeafNodeOption {
	return func(node *LeafNode) {
		node.keys = keys
	}
}

func WithRids(rids []*datapage.RecordID) LeafNodeOption {
	return func(node *LeafNode) {
		node.rids = rids
	}
}

func WithPrevPageNumber(prevPageNumber page.Number) LeafNodeOption {
	return func(node *LeafNode) {
		node.prevPageNumber = prevPageNumber
	}
}

func WithNextPageNumber(nextPageNumber page.Number) LeafNodeOption {
	return func(node *LeafNode) {
		node.nextPageNumber = nextPageNumber
	}
}

// Get the leaf node on which key may reside when queried from node.
//
//nolint:revive // implement the interface method
func (node *LeafNode) Get(key Key) (*LeafNode, error) {
	return node, nil
}

// Put the key and record identifier into the subtree rooted by node.
// If key already exists, raise an error.
func (node *LeafNode) Put(key Key, rid *datapage.RecordID) (*Pair, error) {
	if slices.Contains(node.keys, key) {
		return nil, ErrKeyExists
	}

	insertIndex := util.InsertIndex(key, node.keys)
	node.keys = slices.Insert(node.keys, insertIndex, key)
	node.rids = slices.Insert(node.rids, insertIndex, rid)

	if !node.isOverflowed() {
		if err := node.sync(); err != nil {
			return nil, err
		}
		return nil, nil //nolint:nilnil // nil is returned to indicate no split is needed.
	}

	// Split the node, right node gets the rightmost key and recordID.
	rightKeys := append([]Key{}, node.keys[len(node.keys)-1:]...)
	rightRids := append([]*datapage.RecordID{}, node.rids[len(node.rids)-1:]...)
	rightNode, err := NewLeafNode(
		node.meta,
		node.bufferManager,
		WithKeys(rightKeys),
		WithRids(rightRids),
		WithPrevPageNumber(node.page.PageNumber()),
		WithNextPageNumber(node.nextPageNumber),
	)
	if err != nil {
		return nil, err
	}

	node.keys = node.keys[:len(node.keys)-1]
	node.rids = node.rids[:len(node.rids)-1]
	node.nextPageNumber = rightNode.page.PageNumber()
	if err = node.sync(); err != nil {
		return nil, err
	}

	splitKey := rightKeys[0]
	pair := &Pair{
		Key:   splitKey,
		value: rightNode.page.PageNumber(),
	}
	return pair, nil
}

func (node *LeafNode) sync() error {
	// TODO: implement sync
	return nil
}

func (node *LeafNode) Order() uint32 {
	return node.meta.Order
}

func (node *LeafNode) isOverflowed() bool {
	// FIXME: use the byte size of used space to determine if the node is overflowed.
	return len(node.keys) > int(2*node.meta.Order) //nolint:mnd // 2*order is the threshold.
}

func (node *LeafNode) GetRecordID(key Key) *datapage.RecordID {
	index := slices.Index(node.keys, key)
	if index == -1 {
		return nil
	}

	return node.rids[index]
}
