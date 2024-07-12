package bplustree

import (
	"fmt"
	"slices"
	"strings"

	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/page"
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
	page          *page.DataPage
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
		children:      make([]page.Number, 0, threshold+1),
	}

	node.page = page.NewDataPage(false)
	err := buffManager.ApplyNewPage(meta.tableSpaceID, node.page)
	if err != nil {
		return nil, err
	}
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

func WithInnerKeys(keys []Key) InnerNodeOption {
	return func(node *InnerNode) {
		node.keys = keys
	}
}

func WithChildren(children []page.Number) InnerNodeOption {
	return func(node *InnerNode) {
		node.children = children
	}
}

func WithInnerPage(page *page.DataPage) InnerNodeOption {
	return func(node *InnerNode) {
		node.page = page
	}
}

func WithInnerPrev(prev page.Number) InnerNodeOption {
	return func(node *InnerNode) {
		node.page.SetPrev(prev)
	}
}

func WithInnerNext(next page.Number) InnerNodeOption {
	return func(node *InnerNode) {
		node.page.SetNext(next)
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

func (node *InnerNode) Put(key Key, record *page.Record) (*Pair, error) {
	index := util.SearchIndex(key, node.keys)
	pageNumber := node.children[index]
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
	node.children = slices.Insert(node.children, insertIndex, newPageNum)

	if !node.isOverflowed() {
		if err = node.sync(); err != nil {
			return nil, err
		}
		return nil, nil //nolint:nilnil // nil is returned to indicate no split is needed.
	}

	// When an inner node splits, the first d entries are kept in the left node
	// and the last d entries are moved to the right node.
	// The middle key is moved up to the parent node.
	rightKeys := append([]Key{}, node.keys[node.meta.Order:]...)
	rightChildren := append([]page.Number{}, node.children[node.meta.Order:]...)
	rightNode, err := NewInnerNode(
		node.meta,
		node.bufferManager,
		WithInnerKeys(rightKeys),
		WithChildren(rightChildren),
		WithInnerPrev(node.page.PageNumber()),
		WithInnerNext(node.page.NextPageNumber()),
	)
	if err != nil {
		return nil, err
	}

	node.keys = node.keys[:node.meta.Order]
	node.children = node.children[:node.meta.Order+1]
	node.page.SetPrev(rightNode.page.PageNumber())
	if err = node.sync(); err != nil {
		return nil, err
	}

	splitKey = rightKeys[0]
	return &Pair{key: splitKey, value: rightNode.page.PageNumber()}, nil
}

func (node *InnerNode) PageNumber() page.Number {
	return node.page.PageNumber()
}

func (node *InnerNode) String() string {
	var buffer strings.Builder
	buffer.WriteString("InnerNode(")
	buffer.WriteString(fmt.Sprintf("keys=%v, ", node.keys))
	buffer.WriteString(fmt.Sprintf("children=%v  ", node.children))
	buffer.WriteString(fmt.Sprintf("prev=%d, ", node.page.PrevPageNumber()))
	buffer.WriteString(fmt.Sprintf("next=%d)", node.page.NextPageNumber()))
	return buffer.String()
}

func (node *InnerNode) sync() error {
	return nil
}

func (node *InnerNode) isOverflowed() bool {
	// FIXME: use the byte size of used space to determine if the node is overflowed.
	return len(node.keys) > int(2*node.meta.Order) //nolint:mnd // 2*order is the threshold.
}
