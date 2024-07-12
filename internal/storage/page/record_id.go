package page

import (
	"fmt"
)

// RecordID is the identifier of the record.
type RecordID struct {
	PageNumber Number
	HeapNumber uint16
}

func (r RecordID) String() string {
	return fmt.Sprintf("(%d:%d)", r.PageNumber, r.HeapNumber)
}

func NewRecordID(pageNumber Number, heapNumber uint16) *RecordID {
	return &RecordID{
		PageNumber: pageNumber,
		HeapNumber: heapNumber,
	}
}
