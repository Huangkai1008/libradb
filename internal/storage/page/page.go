// Package page provides the interface for a page and its implementation.
package page

type Type int

const (
	DataPage Type = iota + 1
)

const (
	FileHeaderByteSize  = 38
	FileTrailerByteSize = 8
)

type Number uint32

type Page interface {
	PageNumber() Number
}

type FileHeader struct {
	PageNumber Number
}
