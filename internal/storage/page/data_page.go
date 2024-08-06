package page

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	"github.com/Huangkai1008/libradb/internal/config"
	"github.com/Huangkai1008/libradb/internal/field"
	"github.com/Huangkai1008/libradb/internal/storage/table"
	"github.com/Huangkai1008/libradb/pkg/ds"
)

const (
	DataPageHeaderByteSize = 56
	InfimumHeaderSize      = 5
	InfimumByteSize        = 13
	SupremumHeaderSize     = 5
)

// DataPage is the page that stores data.
// DatePage implements by the heap file.
type DataPage struct {
	mu         sync.RWMutex
	fileHeader *fileHeader
	pageHeader *pageHeader
	// infimumRecord point to the dummy head of the records.
	infimumRecord ds.LinkedList[*Record]
	directory     *directory
	fileTrailer   *fileTrailer
}

func NewDataPage(isLeaf bool) *DataPage {
	p := &DataPage{
		fileHeader: newFileHeader(DataPageType),
		pageHeader: &pageHeader{
			isLeaf: isLeaf,
		},
		infimumRecord: ds.NewDLL[*Record](),
		directory:     newDirectory(),
		fileTrailer:   &fileTrailer{},
	}
	return p
}

func (p *DataPage) PageNumber() Number {
	return p.fileHeader.pageNumber
}

func (p *DataPage) Buffer() []byte {
	return p.ToBytes()
}

func (p *DataPage) IsLeaf() bool {
	return p.pageHeader.isLeaf
}

func (p *DataPage) PrevPageNumber() Number {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.fileHeader.prevPageNumber
}

func (p *DataPage) SetPrev(prevPageNumber Number) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fileHeader.prevPageNumber = prevPageNumber
}

func (p *DataPage) NextPageNumber() Number {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.fileHeader.nextPageNumber
}

func (p *DataPage) SetNext(nextPageNumber Number) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	p.fileHeader.nextPageNumber = nextPageNumber
}

func (p *DataPage) Get(index uint16) *Record {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.infimumRecord.Get(int(index))
}

func (p *DataPage) Insert(index uint16, record *Record) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.infimumRecord.Insert(int(index), record)
	p.pageHeader.recordCount++
}

func (p *DataPage) Append(record *Record) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.infimumRecord.Append(record)
	p.pageHeader.recordCount++
}

// Delete records with given index and returns the record.
func (p *DataPage) Delete(index uint16) *Record {
	p.mu.Lock()
	defer p.mu.Unlock()

	removed := p.infimumRecord.Remove(int(index))
	p.pageHeader.recordCount--
	return removed
}

// Shrink removes all the records start from endIndex and return them.
func (p *DataPage) Shrink(endIndex uint16) []*Record {
	recordCount := p.RecordCount()
	if endIndex >= recordCount {
		return []*Record{}
	}

	records := make([]*Record, recordCount-endIndex)
	for i, j := 0, endIndex; j < recordCount; i, j = i+1, j+1 {
		records[i] = p.Delete(endIndex)
	}
	return records
}

func (p *DataPage) RecordCount() uint16 {
	return p.pageHeader.recordCount
}

func (p *DataPage) keys() []field.Value {
	recordCount := p.RecordCount()
	keys := make([]field.Value, recordCount)
	for i := uint16(0); i < recordCount; i++ {
		keys[i] = p.Get(i).GetKey()
	}
	if p.IsLeaf() {
		return keys
	}

	return keys[1:]
}

// ToBytes converts the data page to a byte slice.
//
// The page format is inspired by the InnoDB page format,
// but more simplified.
//
// The structure of the innodb page is as follows:
//
// +-------------------+
// | File Header       |
// +-------------------+
// | Page Header       |
// +-------------------+
// | Infimum Record    |
// +-------------------+
// | Supremum Record   |
// +-------------------+
// | Records           |
// +-------------------+
// | Directory         |
// +-------------------+
// | File Trailer      |
// +-------------------+ <- Page Size.
func (p *DataPage) ToBytes() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()

	buf := make([]byte, config.PageSize)

	offset := 0
	copy(buf[offset:], p.fileHeader.toBytes())
	offset += FileHeaderByteSize

	copy(buf[offset:], p.pageHeader.toBytes())
	offset += DataPageHeaderByteSize

	recordCount := p.RecordCount()
	for i := uint16(0); i < recordCount; i++ {
		record := p.infimumRecord.Get(int(i))
		recordBytes := record.toBytes()
		copy(buf[offset:], recordBytes)
		offset += len(recordBytes)
	}

	// fileTrailer is from the end of the page.
	copy(buf[config.PageSize-FileTrailerByteSize:], p.fileTrailer.toBytes())
	endOffset := config.PageSize - FileTrailerByteSize

	// directory is from the end of the page, before the file trailer.
	directoryBytes := p.directory.toBytes()
	copy(buf[endOffset-len(directoryBytes):endOffset], directoryBytes)

	return buf
}

func DataPageFromBytes(buf []byte, schema *table.Schema) *DataPage {
	page := NewDataPage(true)

	offset := 0
	page.fileHeader = fileHeaderFromBytes(buf[offset:])
	offset += FileHeaderByteSize

	page.pageHeader = pageHeaderFromBytes(buf[offset:])
	offset += DataPageHeaderByteSize

	recordCount := page.RecordCount()
	for i := uint16(0); i < recordCount; i++ {
		record, recordSize := recordFromBytes(buf[offset:], schema)
		page.Insert(i, record)
		offset += recordSize
	}
	page.pageHeader.recordCount = recordCount

	endOffset := config.PageSize - FileTrailerByteSize
	page.directory = fromBytesDirectory(buf[endOffset-4 : endOffset])

	page.fileTrailer = fileTrailerFromBytes()

	return page
}

func (p *DataPage) String() string {
	var buffer strings.Builder
	buffer.WriteString("DataPage(")
	buffer.WriteString(fmt.Sprintf("number=%v, ", p.PageNumber()))
	buffer.WriteString(fmt.Sprintf("recordCount=%v ", p.RecordCount()))
	buffer.WriteString(fmt.Sprintf("keys=%v)", p.keys()))
	return buffer.String()
}

// pageHeader stores the status information of records stored in a data page.
type pageHeader struct {
	// whether the page is a leaf page.
	isLeaf bool
	// the number of records stored in the page.
	recordCount uint16
}

func (h *pageHeader) toBytes() []byte {
	buf := make([]byte, DataPageHeaderByteSize)
	if h.isLeaf {
		buf[0] = 1
	} else {
		buf[0] = 0
	}
	binary.LittleEndian.PutUint16(buf[1:], h.recordCount)
	return buf
}

func pageHeaderFromBytes(buf []byte) *pageHeader {
	return &pageHeader{
		isLeaf:      buf[0] == 1,
		recordCount: binary.LittleEndian.Uint16(buf[1:]),
	}
}
