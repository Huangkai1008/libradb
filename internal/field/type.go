package field

type TypeID int

const (
	INTEGER TypeID = iota + 1
	VARCHAR
	BOOLEAN
	FLOAT
)

type Type interface {
	// TypeID returns the type id of the field.
	TypeID() TypeID
	// ByteSize returns the byte size of the field.
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
