package field

const DefaultVarcharLength = 255

type Varchar struct {
	allowsNull bool
	// length is the maximum number of characters that can be stored in the VARCHAR field.
	length int
}

func NewVarchar(options ...Option[*Varchar]) *Varchar {
	t := &Varchar{
		length: DefaultVarcharLength,
	}
	applyOptions(t, options...)
	return t
}

func WithLength(length int) Option[*Varchar] {
	return func(t *Varchar) {
		t.length = length
	}
}

func (t *Varchar) TypeID() TypeID {
	return VARCHAR
}

func (t *Varchar) ByteSize() int {
	return t.length * 4
}

func (t *Varchar) Validate() {
	if t.length <= 0 {
		panic("length must be at least 1")
	}
}

func (t *Varchar) AllowNull() bool {
	return t.allowsNull
}

func (t *Varchar) setAllowNull(b bool) {
	t.allowsNull = b
}
