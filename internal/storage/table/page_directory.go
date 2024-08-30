package table

import (
	"encoding/binary"
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

func (d *directory) toBytes() []byte {
	buf := make([]byte, 0)
	offset := 0
	for _, slotOffset := range d.slotOffsets {
		buf = binary.LittleEndian.AppendUint16(buf, slotOffset)
		offset += 2
	}
	return buf
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
