package page

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/Huangkai1008/libradb/internal/config"
	"github.com/Huangkai1008/libradb/internal/field"
)

var (
	ErrRecordNotFound = errors.New("record not found")
)

const (
	DataPageHeaderByteSize = 56
	InfimumHeaderSize      = 5
	InfimumByteSize        = 13
	SupremumHeaderSize     = 5
	SupremumByteSize       = 13
)

const (
	InfimumValue  = 0
	SupremumValue = math.MaxUint64
)

// DataPage is the page that stores data.
// DatePage implements by the heap file.
type DataPage struct {
	fileHeader  *fileHeader
	pageHeader  *pageHeader
	infimum     *infimumRecord
	supremum    *supremumRecord
	records     []*Record
	directory   *directory
	fileTrailer *fileTrailer
}

func NewDataPage(isLeaf bool) *DataPage {
	p := &DataPage{
		fileHeader: newFileHeader(DataPageType),
		pageHeader: &pageHeader{
			isLeaf: isLeaf,
		},
		infimum:     newInfimumRecord(),
		supremum:    newSupremumRecord(),
		directory:   newDirectory(),
		fileTrailer: &fileTrailer{},
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

func (p *DataPage) Unpin() {
	panic("not implemented")
}

func (p *DataPage) PrevPageNumber() Number {
	return p.fileHeader.prevPageNumber
}

func (p *DataPage) SetPrev(prevPageNumber Number) {
	p.fileHeader.prevPageNumber = prevPageNumber
}

func (p *DataPage) NextPageNumber() Number {
	return p.fileHeader.nextPageNumber
}

func (p *DataPage) SetNext(nextPageNumber Number) {
	p.fileHeader.nextPageNumber = nextPageNumber
}

func (p *DataPage) Records() []*Record {
	return p.records
}

func (p *DataPage) SetRecords(records []*Record) {
	p.records = records
}

// GetRecordByKey returns the record by the given key.
func (p *DataPage) GetRecordByKey(key field.Value) (*RecordID, error) {
	slotIndex, err := p.directory.findSlotIndex(key, p.ToBytes())
	if err != nil {
		return nil, err
	}

	// Because the slot offset stores the last record offset,
	// We need to iterate from the last slot.
	slotIndex--
	// Use nextRecordOffset to find the record.
	offset := p.directory.slotOffsets[slotIndex]
	endOffset := p.directory.slotOffsets[slotIndex+1]
	buf := p.Buffer()
	for offset != endOffset {
		recBuf := buf[offset-RecordHeaderByteSize : offset]
		recordHeader := recordHeaderFromBytes(recBuf)
		byteSize := uint16(field.Bytesize(key))
		slotKey, byteErr := field.FromBytes(key.Type(), buf[offset:offset+byteSize])
		if byteErr != nil {
			return nil, byteErr
		}

		if slotKey.Compare(key) == 0 {
			return NewRecordID(p.PageNumber(), recordHeader.heapNumber), nil
		}

		offset = uint16(int(offset) + int(recordHeader.nextRecordOffset))
	}

	return nil, ErrRecordNotFound
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
// +-------------------+
func (p *DataPage) ToBytes() []byte {
	buf := make([]byte, config.PageSize)

	offset := 0
	copy(buf[offset:], p.fileHeader.toBytes())
	offset += FileHeaderByteSize

	copy(buf[offset:], p.pageHeader.toBytes())
	offset += DataPageHeaderByteSize

	copy(buf[offset:], p.infimum.toBytes())
	offset += InfimumByteSize

	copy(buf[offset:], p.supremum.toBytes())
	offset += SupremumByteSize

	// fileTrailer is from the end of the page.
	copy(buf[config.PageSize-FileTrailerByteSize:], p.fileTrailer.toBytes())
	endOffset := config.PageSize - FileTrailerByteSize

	// directory is from the end of the page, before the file trailer.
	copy(buf[:endOffset], p.directory.toBytes())

	return buf
}

func DataPageFromBytes(buf []byte) *DataPage {
	page := &DataPage{}

	offset := 0
	page.fileHeader = fileHeaderFromBytes(buf[offset:])
	offset += FileHeaderByteSize

	page.pageHeader = pageHeaderFromBytes(buf[offset:])
	offset += DataPageHeaderByteSize

	page.infimum = infimumRecordFromBytes(buf[offset:])
	offset += InfimumByteSize

	page.supremum = supremumRecordFromBytes(buf[offset:])
	offset += SupremumByteSize

	page.directory = fromBytesDirectory(buf[offset:])

	page.fileTrailer = fileTrailerFromBytes(buf[config.PageSize-FileTrailerByteSize:])

	return page
}

// pageHeader stores the status information of records stored in a data page.
type pageHeader struct {
	// whether the page is a leaf page.
	isLeaf bool
}

func (h *pageHeader) toBytes() []byte {
	buf := make([]byte, DataPageHeaderByteSize)
	if h.isLeaf {
		buf[0] = 1
	} else {
		buf[0] = 0
	}
	return buf
}

func pageHeaderFromBytes(buf []byte) *pageHeader {
	return &pageHeader{
		isLeaf: buf[0] == 1,
	}
}

// infimumRecord refers to the smallest record in a page.
type infimumRecord struct {
	header *recordHeader
}

func newInfimumRecord() *infimumRecord {
	return &infimumRecord{
		header: &recordHeader{
			recordType: INFIMUM,
		},
	}
}

// toBytes converts the infimum record to a byte slice.
// The infimum record is a fixed InfimumByteSize record.
// The first 5 bytes are the header of the record,
// and the rest of the bytes are the payload of the record.
func (r *infimumRecord) toBytes() []byte {
	buf := make([]byte, InfimumByteSize)
	copy(buf[:InfimumHeaderSize], r.header.toBytes())
	binary.LittleEndian.PutUint64(buf[InfimumHeaderSize:], InfimumValue)
	return buf
}

func infimumRecordFromBytes(buf []byte) *infimumRecord {
	return &infimumRecord{}
}

// supremumRecord refers to the largest record in a page.
type supremumRecord struct {
	header *recordHeader
}

func newSupremumRecord() *supremumRecord {
	return &supremumRecord{
		header: &recordHeader{
			recordType: SUPREMUM,
		},
	}
}

func (r *supremumRecord) toBytes() []byte {
	buf := make([]byte, SupremumByteSize)
	copy(buf[:SupremumHeaderSize], r.header.toBytes())
	binary.LittleEndian.PutUint64(buf[SupremumHeaderSize:], SupremumValue)
	return buf
}

func supremumRecordFromBytes(buf []byte) *supremumRecord {
	return &supremumRecord{}
}
