package bplustree

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/util"
)

var (
	// ErrKeyExists is returned when the key already exists in the leaf node.
	ErrKeyExists = errors.New("key already exists")
)

type LeafNodeOption func(*LeafNode)

// LeafNode is the leaf page in the B+ tree.
type LeafNode struct {
	meta          *Metadata
	page          *page.DataPage
	bufferManager memory.BufferManager

	// keys present the primary key of the record.
	keys []Key
}

func NewLeafNode(
	meta *Metadata,
	buffManager memory.BufferManager,
	options ...LeafNodeOption,
) (*LeafNode, error) {
	threshold := meta.Order * 2 //nolint:mnd // a threshold is the maximum number of keys in the leaf node.
	node := &LeafNode{
		meta:          meta,
		page:          page.NewDataPage(true),
		bufferManager: buffManager,
		keys:          make([]Key, 0, threshold),
	}

	applyLeafNodeOptions(node, options...)
	err := buffManager.ApplyNewPage(meta.tableSpaceID, node.page)
	if err != nil {
		return nil, err
	}

	if err = node.sync(); err != nil {
		return nil, err
	}

	return node, nil
}

func applyLeafNodeOptions(node *LeafNode, options ...LeafNodeOption) {
	for _, option := range options {
		option(node)
	}
}

func WithLeafPrev(prev page.Number) LeafNodeOption {
	return func(node *LeafNode) {
		node.page.SetPrev(prev)
	}
}

func WithLeafNext(next page.Number) LeafNodeOption {
	return func(node *LeafNode) {
		node.page.SetNext(next)
	}
}

func WithDataRecords(records []*page.Record) LeafNodeOption {
	return func(node *LeafNode) {
		node.page.SetRecords(records)
		for _, record := range records {
			node.keys = append(node.keys, record.GetKey())
		}
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
func (node *LeafNode) Put(key Key, record *page.Record) (*Pair, error) {
	if slices.Contains(node.keys, key) {
		return nil, ErrKeyExists
	}

	insertIndex := util.InsertIndex(key, node.keys)
	node.keys = slices.Insert(node.keys, insertIndex, key)

	node.insertRecord(insertIndex, record)

	if !node.isOverflowed() {
		if err := node.sync(); err != nil {
			return nil, err
		}
		return nil, nil //nolint:nilnil // nil is returned to indicate no split is needed.
	}

	// When the leaf splits, it returns the first entry in the right node as the split key.
	// `d` entries remain in the left node; `d + 1` entries are moved to the right node.
	rightKeys := append([]Key{}, node.keys[node.meta.Order:]...)
	rightRecords := append([]*page.Record{}, node.page.Records()[node.meta.Order:]...)
	rightNode, err := NewLeafNode(
		node.meta,
		node.bufferManager,
		WithLeafPrev(node.page.PageNumber()),
		WithLeafNext(node.page.NextPageNumber()),
		WithDataRecords(rightRecords),
	)
	if err != nil {
		return nil, err
	}

	node.keys = node.keys[:node.meta.Order]
	node.page.SetRecords(node.page.Records()[:node.meta.Order])
	node.page.SetNext(rightNode.page.PageNumber())
	if err = node.sync(); err != nil {
		return nil, err
	}

	splitKey := rightKeys[0]
	pair := &Pair{
		key:   splitKey,
		value: rightNode.page.PageNumber(),
	}
	return pair, nil
}

func (node *LeafNode) PageNumber() page.Number {
	return node.page.PageNumber()
}

func leafNodeFromPage(
	meta *Metadata,
	buffManager memory.BufferManager,
	p *page.DataPage,
) *LeafNode {
	node := &LeafNode{
		meta:          meta,
		page:          p,
		bufferManager: buffManager,
	}

	for _, record := range node.page.Records() {
		node.keys = append(node.keys, record.GetKey())
	}

	return node
}

func (node *LeafNode) sync() error {
	return nil
}

func (node *LeafNode) Order() uint32 {
	return node.meta.Order
}

func (node *LeafNode) isOverflowed() bool {
	// FIXME: use the byte size of used space to determine if the node is overflowed.
	return len(node.keys) > int(2*node.meta.Order) //nolint:mnd // 2*order is the threshold.
}

func (node *LeafNode) GetRecord(key Key) *page.Record {
	index := util.FindIndex(key, node.keys)
	if index == -1 {
		return nil
	}

	return node.page.Records()[index]
}

func (node *LeafNode) insertRecord(index int, record *page.Record) {
	node.page.SetRecords(slices.Insert(node.page.Records(), index, record))
}

func (node *LeafNode) String() string {
	var buffer strings.Builder
	buffer.WriteString("LeafNode(")
	buffer.WriteString(fmt.Sprintf("keys=%v, ", node.keys))
	buffer.WriteString(fmt.Sprintf("records=%v  ", node.page.Records()))
	buffer.WriteString(fmt.Sprintf("page=%d, ", node.page.PageNumber()))
	buffer.WriteString(fmt.Sprintf("prev=%d, ", node.page.PrevPageNumber()))
	buffer.WriteString(fmt.Sprintf("next=%d)", node.page.NextPageNumber()))
	return buffer.String()
}
