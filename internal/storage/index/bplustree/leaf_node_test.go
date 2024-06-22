package bplustree_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/Huangkai1008/libradb/internal/field"
	"github.com/Huangkai1008/libradb/internal/storage/index/bplustree"
	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/page/datapage"
)

type testLeaf struct {
	key bplustree.Key
	rid *datapage.RecordID
}

type LeafNodeTestSuite struct {
	suite.Suite
	bufferManager memory.BufferManager
}

func TestLeafTestSuite(t *testing.T) {
	suite.Run(t, new(LeafNodeTestSuite))
}

func (suite *LeafNodeTestSuite) SetupTest() {
	bufferManager := newDummyBufferManager()
	suite.bufferManager = bufferManager
}

func (suite *LeafNodeTestSuite) TearDownTest() {
	_ = suite.bufferManager.Close()
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
		for i := 0; i < 5; i++ { //nolint:intrange // goland not supports
			tests = append(tests, testLeaf{
				key: field.NewValue(typ, i),
				rid: datapage.NewRecordID(1, uint16(i)),
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
		for i := 0; i < 5; i++ {
			tests = append(tests, testLeaf{
				key: field.NewValue(typ, i),
				rid: datapage.NewRecordID(1, uint16(i)),
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
				field.NewValue(typ, i), datapage.NewRecordID(1, uint16(i)),
			)
		}

		for j, k := i, 0; j < i+5; j++ {
			suite.Run(fmt.Sprintf("testLeaf %d", k), func() {
				pair, err := leafNode.Put(
					field.NewValue(typ, j), datapage.NewRecordID(1, uint16(j)),
				)

				suite.Require().NoError(err)
				suite.NotNil(pair)
				suite.Equal(field.NewValue(typ, j), pair.Key())
			})
			k++
		}
	})
}
