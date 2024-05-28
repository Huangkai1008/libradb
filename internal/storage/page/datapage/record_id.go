package datapage

import "github.com/Huangkai1008/libradb/internal/storage/page"

// RecordID is the identifier of the record.
type RecordID struct {
	PageNumber page.Number
	HeapNumber uint16
}
