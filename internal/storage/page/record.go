package page

import (
	"fmt"

	"github.com/Huangkai1008/libradb/internal/field"
)

type RecordType = rune

const RecordHeaderByteSize = 5

const (
	DATA RecordType = iota
	INTERNAL
	INFIMUM
	SUPREMUM
)

// Record is the row format, inspired by the mysql InnoDB `compact` format.
type Record struct {
	header *recordHeader
	values []field.Value
}

//nolint:unused // Ignore unused for now.
type recordHeader struct {
	// deleted is true if the record is deleted, cost 1 bit.
	deleted bool
	// owned_number is the number of the record that owns the heap, cost 4 bits.
	ownedNumber uint8
	// recordType is the type of the record, cost 3 bits.
	recordType RecordType
	// heapNumber is the offset of the record in the page, cost 16 bits.
	heapNumber uint16
	// nextRecordOffset is the offset of the next record in the page, cost 16 bits.
	nextRecordOffset int16
}

func NewRecord(values ...field.Value) *Record {
	return &Record{
		header: &recordHeader{},
		values: values,
	}
}

func NewRecordFromLiteral(values ...any) *Record {
	record := &Record{
		header: &recordHeader{},
		values: make([]field.Value, len(values)),
	}

	for i, value := range values {
		switch v := value.(type) {
		case int:
			record.values[i] = field.NewValue(field.NewInteger(), v)
		case string:
			record.values[i] = field.NewValue(field.NewVarchar(), v)
		case float32, float64:
			record.values[i] = field.NewValue(field.NewFloat(), v)
		case bool:
			record.values[i] = field.NewValue(field.NewBoolean(), v)
		default:
			panic(fmt.Sprintf("unsupported type: %T", v))
		}
	}
	return record
}

func (r *Record) Equal(other *Record) bool {
	if r == nil || other == nil {
		return false
	}

	if len(r.values) != len(other.values) {
		return false
	}

	for i := range r.values {
		if r.values[i] != other.values[i] {
			return false
		}
	}
	return true
}

// func (r *Record) ToBytes(s table.Schema) []byte {
//	var header []byte
//	// Variable length field length list stores the byte length of each variable length field.
//	// Notes the null value of variable length field is not stored in the list.
//	// The list is stored in reverse order.
//	for i := s.Length() - 1; i >= 0; i-- {
//		fieldType, fieldValue := s.FieldTypes[i], r.values[i]
//		if field.IsVarLen(fieldType) && !field.IsNull(fieldValue) {
//			maxLength := field.Length(fieldType)
//			// Set `M` as the maximum character length of the field, and `L` as the actual byte length of the field.
//			// If `M` is greater than 255 and `L` is greater than 127, the field length is stored in 2 bytes.
//			// Otherwise, the field length is stored in 1 byte.
//			byteSize := field.Bytesize(fieldValue)
//			if maxLength*4 > 255 && byteSize > 127 {
//				binary.LittleEndian.AppendUint16(header, uint16(byteSize))
//			} else {
//				header = append(header, byte(byteSize))
//			}
//		}
//	}
//
//	// Null bitmap stores the null value of each field.
//	// The null bitmap is stored in reverse order.
//	// If the field allows NULL value, the corresponding bit is set to 1.
//	// Otherwise, the corresponding bit is set to 0.
//	// If all fields are not NULL, the null bitmap is not stored.
//	nullBitmap := make([]byte, (s.Length()+7)/8)
//	count := 0
//	for i := s.Length() - 1; i >= 0; i-- {
//		fieldType, fieldValue := s.FieldTypes[i], r.values[i]
//		if fieldType.AllowNull() {
//			if field.IsNull(fieldValue) {
//				nullBitmap[count/8] |= 1 << (count % 8)
//			}
//			count++
//		}
//	}
//	// Shrinks the null bitmap to the minimum size.
//	if count > 0 {
//		nullBitmap = nullBitmap[:count/8+1]
//		header = append(header, nullBitmap...)
//	}
//
//	// Record header part toke fixed 5 bytes.
//	header = append(header, []byte{0, 0, 0, 0, 0}...)
//
//	buf := bytes.NewBuffer(nil)
//	for _, fieldValue := range r.values {
//		if !field.IsNull(fieldValue) {
//			buf.Write(fieldValue.ToBytes())
//		}
//	}
//
//	return buf.Bytes()
//}
