package page

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/Huangkai1008/libradb/internal/field"
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

type RecordType = uint8

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
	// deleted is true if the record is deleted, cost 1 byte.
	deleted bool
	// recordType is the type of the record, cost 1 byte.
	recordType RecordType
	// heapNumber is the offset of the record in the page, cost 16 bits.
	heapNumber uint16
	// nextRecordOffset is the offset of the next record in the page, cost 16 bits.
	nextRecordOffset int16
}

func NewRecord(values ...field.Value) *Record {
	return &Record{
		header: &recordHeader{
			deleted: false,
		},
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
		if r.values[i].Compare(other.values[i]) != 0 {
			return false
		}
	}
	return true
}

func (r *Record) Get(i int) field.Value {
	return r.values[i]
}

func (r *Record) Values() []field.Value {
	return r.values
}

func (r *Record) GetKey() field.Value {
	return r.values[0]
}

func (r *Record) String() string {
	return fmt.Sprintf("%v", r.values)
}

func (r *Record) toBytes() []byte {
	// Record header part toke fixed 5 bytes.
	header := make([]byte, RecordHeaderByteSize)
	if r.header.deleted {
		header[0] = 1
	}

	// Store variable length field byte size.
	for _, fieldValue := range r.values {
		if field.IsVarLen(fieldValue.Type()) {
			header = binary.LittleEndian.AppendUint32(header, uint32(len(fieldValue.ToBytes())))
		}
	}

	buf := bytes.NewBuffer(header)
	for _, fieldValue := range r.values {
		if !field.IsNull(fieldValue) {
			buf.Write(fieldValue.ToBytes())
		}
	}

	return buf.Bytes()
}

func recordFromBytes(buf []byte, schema *table.Schema) (*Record, int) {
	offset := 0
	header := &recordHeader{
		deleted: buf[0] == 1,
	}
	offset += RecordHeaderByteSize

	// Get the variable length field byte size.
	fieldTypes := schema.FieldTypes
	varLenFieldSizes := make([]uint32, len(fieldTypes))
	for i, fieldType := range fieldTypes {
		if field.IsVarLen(fieldType) {
			varLenFieldSizes[i] = binary.LittleEndian.Uint32(buf[offset:])
			offset += 4
		}
	}

	values := make([]field.Value, len(fieldTypes))
	for i, fieldType := range fieldTypes {
		if field.IsVarLen(fieldType) {
			values[i], _ = field.FromBytes(fieldType, buf[offset:offset+int(varLenFieldSizes[i])])
			offset += int(varLenFieldSizes[i])
		} else {
			byteSize := fieldTypes[i].ByteSize()
			values[i], _ = field.FromBytes(fieldType, buf[offset:offset+byteSize])
			offset += byteSize
		}
	}
	return &Record{header: header, values: values}, offset
}
