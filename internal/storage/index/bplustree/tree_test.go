package bplustree_test

import (
	"errors"
	"fmt"
	"github.com/Huangkai1008/libradb/internal/field"
	"github.com/Huangkai1008/libradb/internal/storage/page/datapage"
	"math/rand"
	"testing"

	"github.com/Huangkai1008/libradb/internal/storage/index/bplustree"
	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/stretchr/testify/suite"

	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

type dummyPage struct {
	pageNumber page.Number
}

func newDummyPage(pageNumber page.Number) *dummyPage {
	return &dummyPage{
		pageNumber: pageNumber,
	}
}

func (p *dummyPage) PageNumber() page.Number {
	return p.pageNumber
}

func (p *dummyPage) IsLeaf() bool {
	return true
}

// dummyBufferManager is a dummy implementation of memory.BufferManager.
// Use spaceID and pageNumber as the key to store pages.
type dummyBufferManager struct {
	pageMap map[string]page.Page
}

func newDummyBufferManager() *dummyBufferManager {
	return &dummyBufferManager{
		pageMap: make(map[string]page.Page),
	}
}

func (m *dummyBufferManager) ApplyNewPage(spaceID table.SpaceID) (page.Page, error) {
	pageNumber := page.Number(rand.Uint32())
	p := newDummyPage(pageNumber)
	key := fmt.Sprintf("%d:%d", spaceID, pageNumber)
	m.pageMap[key] = p
	return p, nil
}

func (m *dummyBufferManager) FetchPage(spaceID table.SpaceID, pageNumber page.Number) (page.Page, error) {
	key := fmt.Sprintf("%d:%d", spaceID, pageNumber)
	p, ok := m.pageMap[key]
	if !ok {
		return nil, errors.New("page not found")
	}
	return p, nil
}

func (m *dummyBufferManager) Close() error {
	m.pageMap = make(map[string]page.Page)
	return nil
}

type BPlusTreeTestSuite struct {
	suite.Suite
	bufferManager memory.BufferManager
}

func TestBPlusTreeTestSuite(t *testing.T) {
	suite.Run(t, new(BPlusTreeTestSuite))
}

func (suite *BPlusTreeTestSuite) SetupTest() {
	bufferManager := newDummyBufferManager()
	suite.bufferManager = bufferManager
}

func (suite *BPlusTreeTestSuite) TearDownTest() {
	_ = suite.bufferManager.Close()
}

func (suite *BPlusTreeTestSuite) TestNewBPlusTree() {
	suite.Run("should success without error", func() {
		tree, err := bplustree.NewBPlusTree(&bplustree.Metadata{
			Order: 10,
		}, suite.bufferManager)

		suite.Require().NoError(err)
		suite.NotNil(tree)
	})
}

func (suite *BPlusTreeTestSuite) TestBPlusTreePut() {
	suite.Run("should success without error", func() {
		tree, _ := bplustree.NewBPlusTree(&bplustree.Metadata{
			Order: 1,
		}, suite.bufferManager)
		typ := field.NewInteger()

		// (4)
		err := tree.Put(field.NewValue(typ, 4), datapage.NewRecordID(4, 4))
		fmt.Println(tree)
		suite.Require().NoError(err)

		// (4, 9)
		err = tree.Put(field.NewValue(typ, 9), datapage.NewRecordID(9, 9))
		suite.Require().NoError(err)
	})
}
