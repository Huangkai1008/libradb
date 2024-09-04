package disk

import (
	"errors"
	"io"
	"os"

	"github.com/Huangkai1008/libradb/internal/config"
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

type SpaceManager struct {
	dataFile *os.File
}

func NewSpaceManager(dataDir string) (*SpaceManager, error) {
	filePath := dataDir + "/libra.db"
	dataFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &SpaceManager{dataFile: dataFile}, nil
}

func (m *SpaceManager) ReadPage(number table.PageNumber, bytes []byte) error {
	offset := int64(number-1) * int64(config.PageSize)
	_, err := m.dataFile.Seek(offset, 0)
	if err != nil {
		return err
	}

	_, err = m.dataFile.Read(bytes)
	if errors.Is(err, io.EOF) {
		return PageNotAllocated(number)
	}
	return err
}

func (m *SpaceManager) WritePage(number table.PageNumber, bytes []byte) error {
	offset := int64(number-1) * int64(config.PageSize)
	_, err := m.dataFile.Seek(offset, 0)
	if err != nil {
		return err
	}

	_, err = m.dataFile.Write(bytes)
	return err
}

func (m *SpaceManager) Close() error {
	return m.dataFile.Close()
}
