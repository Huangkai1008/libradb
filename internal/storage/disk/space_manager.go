package disk

import (
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

// SpaceManager
type SpaceManager struct {
	dataDir string
}

func NewSpaceManager(dataDir string) *SpaceManager {
	return &SpaceManager{
		dataDir: dataDir,
	}
}

func (m *SpaceManager) ReadPage(number table.PageNumber, bytes []byte) error {
	//TODO implement me
	panic("implement me")
}

func (m *SpaceManager) WritePage(number table.PageNumber, bytes []byte) error {
	//TODO implement me
	panic("implement me")
}
