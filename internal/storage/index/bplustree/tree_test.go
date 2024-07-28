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

//nolint:revive,nilnil // Ignore linter error for now.
func (m *dummyBufferManager) PinPage(spaceID table.SpaceID, pageNumber page.Number) (page.Page, error) {
	return nil, nil
}

//nolint:revive // Ignore linter error for now.
func (m *dummyBufferManager) UnpinPage(spaceID table.SpaceID, pageNumber page.Number) error {
	return nil
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
		schema := table.NewSchema().
			WithField("id", field.NewInteger()).
			WithField("name", field.NewVarchar()).
			WithField("age", field.NewInteger()).
			WithField("is_student", field.NewBoolean()).
			WithField("score", field.NewFloat())

		tree, _ := bplustree.NewBPlusTree(&bplustree.Metadata{
			Order:  1,
			Schema: schema,
		}, suite.bufferManager)
		pkType := field.NewInteger()

		// (4)
		err := tree.Put(field.NewValue(pkType, 4), page.NewRecordFromLiteral(4, "Alice", 20, true, 90.5))
		suite.Require().NoError(err)
		value, err := tree.Get(field.NewValue(pkType, 4))
		suite.Require().NoError(err)
		suite.Equal(page.NewRecordFromLiteral(4, "Alice", 20, true, 90.5), value)

		// (4, 9)
		err = tree.Put(field.NewValue(pkType, 9), page.NewRecordFromLiteral(9, "Bob", 21, false, 85.5))
		suite.Require().NoError(err)
		value, err = tree.Get(field.NewValue(pkType, 9))
		suite.Require().NoError(err)
		suite.Equal(page.NewRecordFromLiteral(9, "Bob", 21, false, 85.5), value)

		//   (6)
		//  /   \
		// (4) (6 9)
		err = tree.Put(field.NewValue(pkType, 6), page.NewRecordFromLiteral(6, "Charlie", 22, true, 80.5))
		suite.Require().NoError(err)
		value, err = tree.Get(field.NewValue(pkType, 6))
		suite.Require().NoError(err)
		suite.Equal(page.NewRecordFromLiteral(6, "Charlie", 22, true, 80.5), value)

		//     (6)
		//    /   \
		// (2 4) (6 9)
		err = tree.Put(field.NewValue(pkType, 2), page.NewRecordFromLiteral(2, "David", 23, false, 75.5))
		suite.Require().NoError(err)
		value, err = tree.Get(field.NewValue(pkType, 2))
		suite.Require().NoError(err)
		suite.Equal(page.NewRecordFromLiteral(2, "David", 23, false, 75.5), value)

		//      (6 7)
		//     /  |  \
		// (2 4) (6) (7 9)
		err = tree.Put(field.NewValue(pkType, 7), page.NewRecordFromLiteral(7, "Eve", 24, true, 70.5))
		suite.Require().NoError(err)
		value, err = tree.Get(field.NewValue(pkType, 7))
		suite.Require().NoError(err)
		suite.Equal(page.NewRecordFromLiteral(7, "Eve", 24, true, 70.5), value)

		//         (7)
		//        /   \
		//     (6)     (8)
		//    /   \   /   \
		// (2 4) (6) (7) (8 9)
		err = tree.Put(field.NewValue(pkType, 8), page.NewRecordFromLiteral(8, "Frank", 25, false, 65.5))
		suite.Require().NoError(err)
		value, err = tree.Get(field.NewValue(pkType, 8))
		suite.Require().NoError(err)
		suite.Equal(page.NewRecordFromLiteral(8, "Frank", 25, false, 65.5), value)

		//            (7)
		//           /   \
		//     (3 6)       (8)
		//   /   |   \    /   \
		// (2) (3 4) (6) (7) (8 9)
		err = tree.Put(field.NewValue(pkType, 3), page.NewRecordFromLiteral(3, "Grace", 26, true, 60.5))
		suite.Require().NoError(err)
		value, err = tree.Get(field.NewValue(pkType, 3))
		suite.Require().NoError(err)
		suite.Equal(page.NewRecordFromLiteral(3, "Grace", 26, true, 60.5), value)
	})
}
