package bplustree

import (
	"slices"

	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/page/datapage"
	"github.com/Huangkai1008/libradb/internal/util"
)

type IndexNodeOption func(*IndexNode)

// IndexNode is the non-leaf page in the B+ tree.
//
// Index pages' records contain keys and child page numbers.
// Since there are at most 2 * Order entries in a node,
// index nodes may have at most 2 * Order +1 child pointers.
// This is also called the tree’s fanout.
//
// See https://cs186berkeley.net/notes/note4/#properties to learn more.
type IndexNode struct {
	meta          *Metadata
	page          page.Page
	bufferManager memory.BufferManager

	// keys present the minimum key on the child page they point to,
	// are sorted in ascending Order.
	keys []Key
	// children present the page number of the child page.
	children []page.Number
	// prevPageNumber and nextPageNumber are the sibling index nodes.
	prevPageNumber page.Number
	nextPageNumber page.Number
}

func NewIndexNode(
	meta *Metadata,
	buffManager memory.BufferManager,
	options ...IndexNodeOption,
) (*IndexNode, error) {
	threshold := meta.Order * 2 //nolint:mnd // a threshold is the maximum number of keys in the index node.
	node := &IndexNode{
		meta:          meta,
		bufferManager: buffManager,
		keys:          make([]Key, 0, threshold),
		children:      make([]page.Number, 0, threshold+1),
	}
	p, err := buffManager.ApplyNewPage(meta.tableSpaceID)
	if err != nil {
		return nil, err
	}
	node.page = p

	applyIndexNodeOptions(node, options...)
	if err = node.sync(); err != nil {
		return nil, err
	}
	return node, nil
}

func applyIndexNodeOptions(node *IndexNode, options ...IndexNodeOption) {
	for _, option := range options {
		option(node)
	}
}

func WithIndexKeys(keys []Key) IndexNodeOption {
	return func(node *IndexNode) {
		node.keys = keys
	}
}

func WithChildren(children []page.Number) IndexNodeOption {
	return func(node *IndexNode) {
		node.children = children
	}
}

func WithIndexPage(page page.Page) IndexNodeOption {
	return func(node *IndexNode) {
		node.page = page
	}
}

func WithIndexPrev(prev page.Number) IndexNodeOption {
	return func(node *IndexNode) {
		node.prevPageNumber = prev
	}
}

func WithIndexNext(next page.Number) IndexNodeOption {
	return func(node *IndexNode) {
		node.nextPageNumber = next
	}
}

// Get the leaf node that may contain the key.
func (node *IndexNode) Get(key Key) (*LeafNode, error) {
	index := util.SearchIndex(key, node.keys)
	pageNumber := node.children[index]
	child, err := BPlusNodeFrom(pageNumber, node.meta, node.bufferManager)
	if err != nil {
		return nil, err
	}

	return child.Get(key)
}

func (node *IndexNode) Put(key Key, rid *datapage.RecordID) (*Pair, error) {
	index := util.SearchIndex(key, node.keys)
	pageNumber := node.children[index]
	child, err := BPlusNodeFrom(pageNumber, node.meta, node.bufferManager)
	if err != nil {
		return nil, err
	}

	pair, err := child.Put(key, rid)
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

	// Split the node, right node gets the rightmost key and children.
	rightKeys := append([]Key{}, node.keys[len(node.keys)-1])
	rightChildren := append([]page.Number{}, node.children[len(node.children)-1])
	rightNode, err := NewIndexNode(
		node.meta,
		node.bufferManager,
		WithIndexKeys(rightKeys),
		WithChildren(rightChildren),
		WithIndexPrev(node.page.PageNumber()),
		WithIndexNext(node.nextPageNumber),
	)
	if err != nil {
		return nil, err
	}

	// Remove the rightmost key and children from the left node.
	node.keys = node.keys[:len(node.keys)-1]
	node.children = node.children[:len(node.children)-1]
	node.nextPageNumber = rightNode.page.PageNumber()
	if err = node.sync(); err != nil {
		return nil, err
	}

	splitKey = rightKeys[0]
	return &Pair{key: splitKey, value: rightNode.page.PageNumber()}, nil
}

func (node *IndexNode) PageNumber() page.Number {
	return node.page.PageNumber()
}

func (node *IndexNode) sync() error {
	// TODO: implement sync
	return nil
}

func (node *IndexNode) isOverflowed() bool {
	// FIXME: use the byte size of used space to determine if the node is overflowed.
	return len(node.keys) > int(2*node.meta.Order) //nolint:mnd // 2*order is the threshold.
}
