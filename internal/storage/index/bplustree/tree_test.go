package bplustree_test

import (
	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/table"
	"github.com/stretchr/testify/mock"
)

type DummyBufferManager struct {
	mock.Mock
}

func (m *DummyBufferManager) ApplyNewPage(spaceID table.SpaceID) (page.Page, error) {
	args := m.Called(spaceID)
	return args.Get(0).(page.Page), args.Error(1)
}

func (m *DummyBufferManager) FetchPage(spaceID table.SpaceID, pageNumber page.Number) (page.Page, error) {
	args := m.Called(spaceID, pageNumber)
	return args.Get(0).(page.Page), args.Error(1)
}

func (m *DummyBufferManager) Close() error {
	args := m.Called()
	return args.Error(0)
}
