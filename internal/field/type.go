package field

type TypeID int

const (
	INTEGER TypeID = iota + 1
	BIGINT
	FLOAT
	CHAR
	VARCHAR
	BOOLEAN
)

type Type interface {
	// TypeID returns the type id of the field.
	TypeID() TypeID
	// AllowNull returns whether the field allows null value.
	AllowNull() bool
	// ByteSize returns the byte size of the field.
	ByteSize() int
}

type Option[T Type] func(T)

func applyOptions[T Type](t T, options ...Option[T]) {
	for _, option := range options {
		option(t)
	}
}
