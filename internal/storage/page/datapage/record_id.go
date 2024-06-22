package datapage

import "github.com/Huangkai1008/libradb/internal/storage/page"

// RecordID is the identifier of the record.
type RecordID struct {
	PageNumber page.Number
	HeapNumber uint16
}

func NewRecordID(pageNumber page.Number, heapNumber uint16) *RecordID {
	return &RecordID{
		PageNumber: pageNumber,
		HeapNumber: heapNumber,
	}
}
