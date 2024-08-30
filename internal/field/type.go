package field

type TypeID uint8

const (
	INTEGER TypeID = iota + 1
	VARCHAR
	BOOLEAN
	FLOAT
	BINARY
)

type Type interface {
	// TypeID returns the type id of the field.
	TypeID() TypeID
	// ByteSize returns the byte size of the field.
	// If the field is a variable-length field, the byte size is calculated based on the value.
	ByteSize() int
	Validator
	Nullable
}

type Validator interface {
	Validate()
}

type Nullable interface {
	AllowNull() bool
	setAllowNull(bool)
}

type Option[T any] func(T)

func applyOptions[T Type](t T, options ...Option[T]) {
	for _, option := range options {
		option(t)
	}
	t.Validate()
}

func WithAllowNull[T Nullable](allowNull bool) Option[T] {
	return func(t T) {
		t.setAllowNull(allowNull)
	}
}

func IsVarLen(t Type) bool {
	return t.TypeID() == VARCHAR || t.TypeID() == BINARY
}

// Length returns the length of the field.
//
// Note: This function is only applicable to Variable-Length fields.
func Length(t Type) int {
	if t.TypeID() != VARCHAR && t.TypeID() != BINARY {
		panic("unsupported type")
	}

	return t.(*Varchar).length
}
