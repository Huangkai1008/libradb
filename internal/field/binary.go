package field

const DefaultBinaryLength = 128

type Binary struct {
	allowsNull bool
	// length is the maximum number of bytes that can be stored in the BINARY field.
	length int
}

func NewBinary(options ...Option[*Binary]) *Binary {
	t := &Binary{
		length: DefaultBinaryLength,
	}
	applyOptions(t, options...)
	return t
}

func WithByteLength(length int) Option[*Binary] {
	return func(t *Binary) {
		t.length = length
	}
}

func (t *Binary) TypeID() TypeID {
	return BINARY
}

func (t *Binary) PerByteSize() int {
	return 1
}

func (t *Binary) ByteSize() int {
	return t.length
}

func (t *Binary) Validate() {
	if t.length <= 0 {
		panic("length must be at least 1")
	}
}

func (t *Binary) AllowNull() bool {
	return t.allowsNull
}

func (t *Binary) setAllowNull(b bool) {
	t.allowsNull = b
}

func (t *Binary) Length() int {
	return t.length
}
