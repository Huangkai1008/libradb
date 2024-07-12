package page

import (
	"encoding/binary"

	"github.com/Huangkai1008/libradb/internal/field"
)

// We divide the normal records info groups, each group has a slot.
// The pageOffset is the address offset of the last record in the group on the page.
// slotOffsets sorted in ascending order.
type directory struct {
	slotOffsets []pageOffset
}

// newDirectory creates a new directory.
// In the beginning, the directory only has two slots,
// one for the infimum record and the other for the supremum record.
func newDirectory() *directory {
	dir := &directory{
		slotOffsets: []pageOffset{},
	}

	// The first slot is for the infimum record.
	dir.slotOffsets = append(dir.slotOffsets, FileHeaderByteSize+DataPageHeaderByteSize+InfimumHeaderSize)
	// The second slot is for the supremum record.
	dir.slotOffsets = append(dir.slotOffsets, dir.slotOffsets[0]+(InfimumByteSize-InfimumHeaderSize)+SupremumHeaderSize)
	return dir
}

func (d *directory) size() int {
	return len(d.slotOffsets)
}

func (d *directory) toBytes() []byte {
	buf := make([]byte, 0)
	offset := 0
	for _, slotOffset := range d.slotOffsets {
		buf = binary.LittleEndian.AppendUint16(buf, slotOffset)
		offset += 2
	}
	return buf
}

// findSlotIndex use binary search to find the index in the page directory by key.
func (d *directory) findSlotIndex(key field.Value, buf []byte) (int, error) {
	left, right := 0, d.size()
	for left < right {
		mid := int(uint(left+right) >> 1)
		offset := d.slotOffsets[mid]

		recBuf := buf[offset-RecordHeaderByteSize : offset]
		recordHeader := recordHeaderFromBytes(recBuf)
		recordType := recordHeader.recordType
		if recordType == INFIMUM {
			left = mid + 1
		} else if recordType == SUPREMUM {
			right = mid
		} else {
			byteSize := uint16(field.Bytesize(key))
			slotKey, err := field.FromBytes(key.Type(), buf[offset:offset+byteSize])
			if err != nil {
				return -1, err
			}

			if slotKey.Compare(key) < 0 {
				left = mid + 1
			} else {
				right = mid
			}
		}
	}
	return left, nil
}

func fromBytesDirectory(buf []byte) *directory {
	dir := &directory{
		slotOffsets: []pageOffset{},
	}
	offset := 0
	for i := 0; i < len(buf); i += 2 {
		dir.slotOffsets = append(dir.slotOffsets, binary.LittleEndian.Uint16(buf[offset:offset+2]))
		offset += 2
	}
	return dir
}
