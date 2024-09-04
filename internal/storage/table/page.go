package table

import (
	"encoding/binary"
	"os"
	"strconv"
	"sync"
)

type Type = uint16

const (
	DataPageType Type = iota + 1
)

const (
	FileHeaderByteSize  = 38
	FileTrailerByteSize = 8
)

var newPageNumberGenerator = sync.OnceValue(NewPageNumberGenerator) //nolint:gochecknoglobals // Global variable for the page number generator.

type PageNumber uint32

const (
	InvalidPageNumber   = PageNumber(0)
	CurPageNumberEnvKey = "CUR_PAGE_NUMBER"
)

type PageNumberGenerator struct {
	curPageNumber PageNumber
	mu            sync.Mutex
}

func NewPageNumberGenerator() *PageNumberGenerator {
	// Get the current page number from the environment variable
	curPageNumber := os.Getenv(CurPageNumberEnvKey)

	// Default to 0 if the environment variable is missing or invalid
	pageNumber, err := strconv.Atoi(curPageNumber)
	if err != nil {
		pageNumber = 0
	}

	// Initialize the generator with the determined page number
	return &PageNumberGenerator{
		curPageNumber: PageNumber(pageNumber),
	}
}

func (g *PageNumberGenerator) NextPageNumber() PageNumber {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.curPageNumber++
	return g.curPageNumber
}

type pageOffset = uint16

// Page represents a page in the storage.
type Page interface {
	// PageNumber returns the page number.
	PageNumber() PageNumber
	// Buffer returns the byte slice of the page.
	Buffer() []byte
}

func FromBytes(buf []byte, s *Schema) Page {
	header := fileHeaderFromBytes(buf)
	if header.pageType == DataPageType {
		return DataPageFromBytes(buf, s)
	}

	panic("Invalid page type")
}

// fileHeader represents the header of a file.
type fileHeader struct {
	// The page number is a unique identifier for the page.
	pageNumber PageNumber
	// pageType of the page.
	pageType Type
	// The prevPageNumber is the page number of the previous page in the file.
	prevPageNumber PageNumber
	// The nextPageNumber is the page number of the next page in the file.
	nextPageNumber PageNumber
}

func newFileHeader(pageType Type) *fileHeader {
	g := newPageNumberGenerator()
	return &fileHeader{
		pageNumber: g.NextPageNumber(),
		pageType:   pageType,
	}
}

func (h *fileHeader) toBytes() []byte {
	buf := make([]byte, FileHeaderByteSize)
	offset := 0
	// The first 4 bytes are the page number.
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(h.pageNumber))
	offset += 4
	// The next 2 bytes are the page type.
	binary.LittleEndian.PutUint16(buf[offset:offset+2], h.pageType)
	offset += 2
	// The next 4 bytes are the prevPageNumber.
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(h.prevPageNumber))
	offset += 4
	// The next 4 bytes are the nextPageNumber.
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(h.nextPageNumber))
	// The next 24 bytes are reserved for future use.
	return buf
}

// fileHeaderFromBytes creates a fileHeader from a byte slice.
// The fileHeader took the first FileHeaderByteSize bytes of a page.
// Diff from the INNODB page format, the first 4 bytes are the page number,
// and the next 2 bytes are the page type.
func fileHeaderFromBytes(buf []byte) *fileHeader {
	offset := 0
	// The first 4 bytes are the page number.
	pageNumber := PageNumber(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4
	// The next 2 bytes are the page type.
	pageType := binary.LittleEndian.Uint16(buf[offset : offset+2])
	offset += 2
	// The next 4 bytes are the prevPageNumber.
	prevPageNumber := PageNumber(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	offset += 4
	// The next 4 bytes are the nextPageNumber.
	nextPageNumber := PageNumber(binary.LittleEndian.Uint32(buf[offset : offset+4]))
	return &fileHeader{
		pageNumber:     pageNumber,
		pageType:       pageType,
		prevPageNumber: prevPageNumber,
		nextPageNumber: nextPageNumber,
	}
}

// fileTrailer serves to validate the integrity of the page,
// ensuring that its content remains unchanged
// after the page is flushed from memory to disk.
type fileTrailer struct {
}

func (t *fileTrailer) toBytes() []byte {
	buf := make([]byte, FileTrailerByteSize)
	return buf
}

// fileTrailerFromBytes creates a fileTrailer from a byte slice.
// The fileTrailer took the last FileTrailerByteSize bytes of a page.
func fileTrailerFromBytes() *fileTrailer {
	return &fileTrailer{}
}
