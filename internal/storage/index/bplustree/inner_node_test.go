package bplustree_test

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/Huangkai1008/libradb/internal/storage/index/bplustree"
	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/page"
)

type testInner struct {
	key bplustree.Key
}

type dummyInnerPage struct {
	mock.Mock
}

func (d *dummyInnerPage) PageNumber() page.Number {
	return page.Number(rand.Uint32())
}

func (d *dummyInnerPage) IsLeaf() bool {
	return false
}

type InnerNodeTestSuite struct {
	suite.Suite
	bufferManager memory.BufferManager
}

func TestInnerTestSuite(t *testing.T) {
	suite.Run(t, new(InnerNodeTestSuite))
}

func (suite *InnerNodeTestSuite) getEmptyInnerNode() (*bplustree.InnerNode, error) {
	innerNode, err := bplustree.NewInnerNode(&bplustree.Metadata{
		Order: 10,
	}, suite.bufferManager)
	return innerNode, err
}

func (suite *InnerNodeTestSuite) SetupTest() {
	bufferManager := new(DummyBufferManager)
	bufferManager.On("ApplyNewPage", mock.Anything).Return(new(dummyInnerPage), nil)
	bufferManager.On("FetchPage", mock.Anything, mock.Anything).Return(new(dummyInnerPage), nil)
	bufferManager.On("Close").Return(nil)

	suite.bufferManager = bufferManager
}

func (suite *InnerNodeTestSuite) TearDownTest() {
	_ = suite.bufferManager.Close()
}

func (suite *InnerNodeTestSuite) TestNewInnerNode() {
	suite.Run("should success without error", func() {
		innerNode, err := suite.getEmptyInnerNode()

		suite.Require().NoError(err)
		suite.NotNil(innerNode)
	})
}
