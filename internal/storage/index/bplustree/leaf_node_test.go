package bplustree_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/Huangkai1008/libradb/internal/field"
	"github.com/Huangkai1008/libradb/internal/storage/index/bplustree"
	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

type testLeaf struct {
	key    bplustree.Key
	record *page.Record
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
	schema := table.NewSchema().
		WithField("id", field.NewInteger()).
		WithField("name", field.NewVarchar()).
		WithField("age", field.NewInteger()).
		WithField("is_student", field.NewBoolean()).
		WithField("score", field.NewFloat())
	leafNode, err := bplustree.NewLeafNode(&bplustree.Metadata{
		Order:  10,
		Schema: schema,
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
		primaryType := field.NewInteger()
		var tests []testLeaf
		for i := 0; i < 5; i++ { //nolint:intrange // goland not supports
			key := field.NewValue(primaryType, i)
			tests = append(tests, testLeaf{
				key:    key,
				record: page.NewRecordFromLiteral(i, fmt.Sprintf("name-%d", i), i, i%2 == 0, float64(i)),
			})
		}

		for i, tt := range tests {
			suite.Run(fmt.Sprintf("testLeaf %d", i), func() {
				pair, err := leafNode.Put(tt.key, tt.record)

				suite.Require().NoError(err)
				suite.Nil(pair)

				getRecord := leafNode.GetRecord(tt.key)
				suite.Require().NotNil(getRecord)
				suite.True(getRecord.Equal(tt.record))
			})
		}
	})

	suite.Run("should raise error when put duplicate key", func() {
		leafNode, _ := suite.getEmptyLeafNode()
		primaryType := field.NewInteger()
		var tests []testLeaf
		for i := 0; i < 5; i++ {
			key := field.NewValue(primaryType, i)
			tests = append(tests, testLeaf{
				key:    key,
				record: page.NewRecordFromLiteral(i, fmt.Sprintf("name-%d", i), i, i%2 == 0, float64(i)),
			})
		}

		for i, tt := range tests {
			suite.Run(fmt.Sprintf("testLeaf %d", i), func() {
				_, err := leafNode.Put(tt.key, tt.record)

				suite.Require().NoError(err)

				pair, err := leafNode.Put(tt.key, tt.record)
				suite.Require().Error(err)
				suite.Nil(pair)
			})
		}
	})

	suite.Run("should split when put overflowed", func() {
		leafNode, _ := suite.getEmptyLeafNode()
		primaryType := field.NewInteger()
		i := 0
		var tests []testLeaf
		for ; i < suite.timesToOverflow(leafNode); i++ {
			key := field.NewValue(primaryType, i)
			tests = append(tests, testLeaf{
				key:    key,
				record: page.NewRecordFromLiteral(i, fmt.Sprintf("name-%d", i), i, i%2 == 0, float64(i)),
			})
		}

		for _, tt := range tests {
			_, _ = leafNode.Put(tt.key, tt.record)
		}

		pair, err := leafNode.Put(
			field.NewValue(primaryType, i),
			page.NewRecordFromLiteral(i, fmt.Sprintf("name-%d", i), i, i%2 == 0, float64(i)),
		)

		suite.Require().NoError(err)
		suite.NotNil(pair)
	})
}
