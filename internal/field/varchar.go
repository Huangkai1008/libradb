package field

import "errors"

const DefaultVarcharLength = 255

type Varchar struct {
	allowsNull bool
	// length is the maximum number of characters that can be stored in the VARCHAR field.
	length int
}

type VarcharOption func(*Varchar)

func NewVarchar(options ...Option[*Varchar]) (*Varchar, error) {
	t := &Varchar{
		length: DefaultVarcharLength,
	}
	applyOptions(t, options...)

	if t.length <= 0 {
		return nil, errors.New("length must be at least 1")
	}
	return t, nil
}

func WithLength(length int) Option[*Varchar] {
	return func(t *Varchar) {
		t.length = length
	}
}

func (t *Varchar) TypeID() TypeID {
	return VARCHAR
}

func (t *Varchar) AllowNull() bool {
	return t.allowsNull
}

func (t *Varchar) ByteSize() int {
	return t.length * 4
}
