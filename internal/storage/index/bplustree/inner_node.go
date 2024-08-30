package bplustree

import (
	"fmt"
	"slices"
	"strings"

	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/table"
	"github.com/Huangkai1008/libradb/internal/util"
)

type InnerNodeOption func(*InnerNode)

// InnerNode is the non-leaf page in the B+ tree.
//
// Inner pages' records contain keys and child page numbers.
// Since there are at most 2 * Order entries in a node,
// inner nodes may have at most 2 * Order +1 child pointers.
// This is also called the tree’s fanout.
//
// See https://cs186berkeley.net/notes/note4/#properties to learn more.
type InnerNode struct {
	meta          *Metadata
	page          *table.DataPage
	bufferManager memory.BufferManager

	// keys present the minimum key on the child page they point to,
	// are sorted in ascending Order.
	keys []Key
	// children present the child page numbers.
	children []table.PageNumber
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
		children:      make([]table.PageNumber, 0, threshold+1),
	}

	node.page = table.NewDataPage(false)
	err := buffManager.ApplyNewPage(meta.tableSpaceID, node.page)
	if err != nil {
		return nil, err
	}
	applyInnerNodeOptions(node, options...)
	return node, nil
}

func applyInnerNodeOptions(node *InnerNode, options ...InnerNodeOption) {
	for _, option := range options {
		option(node)
	}
}

func WithInnerPrev(prev table.PageNumber) InnerNodeOption {
	return func(node *InnerNode) {
		node.page.SetPrev(prev)
	}
}

func WithInnerNext(next table.PageNumber) InnerNodeOption {
	return func(node *InnerNode) {
		node.page.SetNext(next)
	}
}

func WithIndexRecords(records []*table.Record) InnerNodeOption {
	return func(node *InnerNode) {
		for _, record := range records {
			node.page.Append(record)
		}
		for i, record := range records {
			if i > 0 {
				node.keys = append(node.keys, record.GetKey())
			}

			val := record.Get(1).Val()
			node.children = append(node.children, table.PageNumber(val.(int32)))
		}
	}
}

// Get the leaf node that may contain the key.
func (node *InnerNode) Get(key Key) (*LeafNode, error) {
	index := util.SearchIndex(key, node.keys)
	pageNumber := node.getChild(index)
	node.unpin(false)
	child, err := BPlusNodeFrom(pageNumber, node.meta, node.bufferManager)
	if err != nil {
		return nil, err
	}

	return child.Get(key)
}

func (node *InnerNode) Put(key Key, record *table.Record) (*Pair, error) {
	defer node.unpin(true)

	index := util.SearchIndex(key, node.keys)
	pageNumber := node.getChild(index)
	child, err := BPlusNodeFrom(pageNumber, node.meta, node.bufferManager)
	if err != nil {
		return nil, err
	}

	pair, err := child.Put(key, record)
	if err != nil {
		return nil, err
	}

	// If puts the pair (k, r) does not cause the node to overflow.
	if pair == nil {
		return nil, nil //nolint:nilnil // nil is returned to indicate no split is needed.
	}

	splitKey, newPageNum := pair.key, pair.value
	insertIndex := util.InsertIndex(splitKey, node.keys)
	node.keys = slices.Insert(node.keys, insertIndex, splitKey)
	node.children = slices.Insert(node.children, insertIndex+1, newPageNum)
	indexRecord := newIndexRecord(splitKey, newPageNum)
	node.insertRecord(insertIndex+1, indexRecord)

	if !node.isOverflowed() {
		return nil, nil //nolint:nilnil // nil is returned to indicate no split is needed.
	}

	// When an inner node splits, the first d entries are kept in the left node
	// and the last d entries are moved to the right node.
	// The middle key is moved up to the parent node.
	splitKey = node.keys[node.meta.Order]
	rightRecords := node.page.Shrink(node.meta.Order + 1)
	rightNode, err := NewInnerNode(
		node.meta,
		node.bufferManager,
		WithInnerPrev(node.page.PageNumber()),
		WithInnerNext(node.page.NextPageNumber()),
		WithIndexRecords(rightRecords),
	)
	if err != nil {
		return nil, err
	}

	node.keys = node.keys[:node.meta.Order]
	node.children = node.children[:node.meta.Order+1]
	node.page.SetPrev(rightNode.page.PageNumber())
	node.unpin(true)
	return &Pair{key: splitKey, value: rightNode.page.PageNumber()}, nil
}

func (node *InnerNode) Delete(key Key) error {
	leafNode, err := node.Get(key)
	if err != nil {
		return err
	}

	node.unpin(false)
	return leafNode.Delete(key)
}

func (node *InnerNode) PageNumber() table.PageNumber {
	return node.page.PageNumber()
}

func innerNodeFromPage(
	meta *Metadata,
	buffManager memory.BufferManager,
	p *table.DataPage,
) *InnerNode {
	node := &InnerNode{
		meta:          meta,
		page:          p,
		bufferManager: buffManager,
	}

	recordCount := node.page.RecordCount()
	for i := uint16(0); i < recordCount; i++ {
		record := node.page.Get(i)
		if i > 0 {
			node.keys = append(node.keys, record.GetKey())
		}
		val := record.Get(1).Val()
		node.children = append(node.children, table.PageNumber(val.(int32)))
	}
	return node
}

func (node *InnerNode) getChild(index int) table.PageNumber {
	return node.children[index]
}

func (node *InnerNode) insertRecord(index int, record *table.Record) {
	node.page.Insert(uint16(index), record)
}

func (node *InnerNode) unpin(markDirty bool) {
	node.bufferManager.Unpin(node.PageNumber(), markDirty)
}

func (node *InnerNode) String() string {
	var buffer strings.Builder
	buffer.WriteString("InnerNode(")
	buffer.WriteString(fmt.Sprintf("keys=%v, ", node.keys))
	buffer.WriteString(fmt.Sprintf("children=%v  ", node.children))
	buffer.WriteString(fmt.Sprintf("page=%d, ", node.page.PageNumber()))
	buffer.WriteString(fmt.Sprintf("prev=%d, ", node.page.PrevPageNumber()))
	buffer.WriteString(fmt.Sprintf("next=%d)", node.page.NextPageNumber()))
	return buffer.String()
}

func (node *InnerNode) isOverflowed() bool {
	// FIXME: use the byte size of used space to determine if the node is overflowed.
	return len(node.keys) > int(2*node.meta.Order) //nolint:mnd // 2*order is the threshold.
}
