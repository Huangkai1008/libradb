package bplustree_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/Huangkai1008/libradb/internal/field"
	"github.com/Huangkai1008/libradb/internal/storage/index/bplustree"
	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

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

func (m *dummyBufferManager) ApplyNewPage(spaceID table.SpaceID, p page.Page) error {
	key := fmt.Sprintf("%d:%d", spaceID, p.PageNumber())
	m.pageMap[key] = p
	return nil
}

func (m *dummyBufferManager) FetchPage(spaceID table.SpaceID, pageNumber page.Number) (page.Page, error) {
	key := fmt.Sprintf("%d:%d", spaceID, pageNumber)
	p, ok := m.pageMap[key]
	if !ok {
		return nil, errors.New("page not found")
	}
	return p, nil
}

func (m *dummyBufferManager) PinPage(spaceID table.SpaceID, pageNumber page.Number) (page.Page, error) {
	//TODO implement me
	panic("implement me")
}

func (m *dummyBufferManager) UnpinPage(spaceID table.SpaceID, pageNumber page.Number) error {
	//TODO implement me
	panic("implement me")
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
		pkType := field.NewInteger()

		// (4)
		err := tree.Put(field.NewValue(pkType, 4), page.NewRecordFromLiteral(4, 4))
		suite.Require().NoError(err)

		// (4, 9)
		err = tree.Put(field.NewValue(pkType, 9), page.NewRecordFromLiteral(4, 4))
		suite.Require().NoError(err)

		//   (6)
		//  /   \
		// (4) (6 9)
		err = tree.Put(field.NewValue(pkType, 6), page.NewRecordFromLiteral(4, 4))
		suite.Require().NoError(err)

		//     (6)
		//    /   \
		// (2 4) (6 9)
		err = tree.Put(field.NewValue(pkType, 2), page.NewRecordFromLiteral(4, 4))
		suite.Require().NoError(err)

		//      (6 7)
		//     /  |  \
		// (2 4) (6) (7 9)
		err = tree.Put(field.NewValue(pkType, 7), page.NewRecordFromLiteral(4, 4))
	})
}
