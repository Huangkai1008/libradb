package page

import (
	"math"

	"github.com/Huangkai1008/libradb/internal/config"
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
	buf := make([]byte, config.PageSize)

	offset := 0
	copy(buf[offset:], p.fileHeader.toBytes())
	offset += FileHeaderByteSize

	copy(buf[offset:], p.pageHeader.toBytes())

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

	page.directory = fromBytesDirectory(buf[offset:])

	page.fileTrailer = fileTrailerFromBytes()

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
