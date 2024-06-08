package bplustree_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/Huangkai1008/libradb/internal/field"
	"github.com/Huangkai1008/libradb/internal/storage/index/bplustree"
	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/page/datapage"
)

type testLeaf struct {
	key bplustree.Key
	rid *datapage.RecordID
}

type dummyLeafPage struct {
	mock.Mock
}

func (d *dummyLeafPage) PageNumber() page.Number {
	return page.Number(rand.Uint32())
}

func (d *dummyLeafPage) IsLeaf() bool {
	return true
}

type LeafNodeTestSuite struct {
	suite.Suite
	bufferManager memory.BufferManager
}

func TestLeafTestSuite(t *testing.T) {
	suite.Run(t, new(LeafNodeTestSuite))
}

func (suite *LeafNodeTestSuite) getEmptyLeafNode() (*bplustree.LeafNode, error) {
	leafNode, err := bplustree.NewLeafNode(&bplustree.Metadata{
		Order: 10,
	}, suite.bufferManager)
	return leafNode, err
}

func (suite *LeafNodeTestSuite) timesToOverflow(node *bplustree.LeafNode) int {
	return int(node.Order() * 2)
}

func (suite *LeafNodeTestSuite) SetupTest() {
	bufferManager := new(DummyBufferManager)
	bufferManager.On("ApplyNewPage", mock.Anything).Return(new(dummyLeafPage), nil)
	bufferManager.On("FetchPage", mock.Anything, mock.Anything).Return(new(dummyLeafPage), nil)
	bufferManager.On("Close").Return(nil)

	suite.bufferManager = bufferManager
}

func (suite *LeafNodeTestSuite) TearDownTest() {
	_ = suite.bufferManager.Close()
}

func (suite *LeafNodeTestSuite) TestNewLeafNode() {
	suite.Run("should success without error", func() {
		leafNode, err := suite.getEmptyLeafNode()

		suite.Require().NoError(err)
		suite.NotNil(leafNode)
	})
}

func (suite *LeafNodeTestSuite) TestLeafNodePut() {
	suite.Run("should not split when put not overflowed", func() {
		leafNode, _ := suite.getEmptyLeafNode()

		typ := &field.Integer{}

		var tests []testLeaf

		for i := 0; i < suite.timesToOverflow(leafNode); i++ { //nolint:intrange // goland not supports
			tests = append(tests, testLeaf{
				key: field.NewValue(typ, int32(i)),
				rid: &datapage.RecordID{PageNumber: 1, HeapNumber: uint16(i)},
			})
		}

		for i, tt := range tests {
			suite.Run(fmt.Sprintf("testLeaf %d", i), func() {
				pair, err := leafNode.Put(tt.key, tt.rid)

				suite.Require().NoError(err)
				suite.Nil(pair)
				suite.Equal(tt.rid, leafNode.GetRecordID(tt.key))
			})
		}
	})

	suite.Run("should raise error when put duplicate key", func() {
		leafNode, _ := suite.getEmptyLeafNode()

		typ := &field.Integer{}

		var tests []testLeaf

		i := 0
		for ; i < suite.timesToOverflow(leafNode); i++ {
			tests = append(tests, testLeaf{
				key: field.NewValue(typ, int32(i)),
				rid: &datapage.RecordID{PageNumber: 1, HeapNumber: uint16(i)},
			})
		}

		for i, tt := range tests {
			suite.Run(fmt.Sprintf("testLeaf %d", i), func() {
				_, err := leafNode.Put(tt.key, tt.rid)

				suite.Require().NoError(err)

				pair, err := leafNode.Put(tt.key, tt.rid)
				suite.Require().Error(err)
				suite.Nil(pair)
			})
		}
	})

	suite.Run("should split when put overflowed", func() {
		leafNode, _ := suite.getEmptyLeafNode()

		typ := &field.Integer{}

		i := 0
		for ; i < suite.timesToOverflow(leafNode); i++ {
			_, _ = leafNode.Put(
				field.NewValue(typ, int32(i)), &datapage.RecordID{PageNumber: 1, HeapNumber: uint16(i)},
			)
		}

		for j, k := i, 0; j < i+10; j++ {
			suite.Run(fmt.Sprintf("testLeaf %d", k), func() {
				pair, err := leafNode.Put(
					field.NewValue(typ, int32(j)), &datapage.RecordID{PageNumber: 1, HeapNumber: uint16(j)},
				)

				suite.Require().NoError(err)
				suite.NotNil(pair)
				suite.Equal(field.NewValue(typ, int32(j)), pair.Key)
			})
			k++
		}
	})
}
