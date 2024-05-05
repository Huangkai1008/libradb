package field

type Integer struct {
	allowNull bool
}

func NewInteger(options ...Option[*Integer]) *Integer {
	t := &Integer{}
	applyOptions(t, options...)
	return t
}

func (t *Integer) TypeID() TypeID {
	return INTEGER
}

func (t *Integer) ByteSize() int {
	return 4 // //nolint:mnd // 4 bytes for an int32
}

func (t *Integer) Validate() {
}

func (t *Integer) AllowNull() bool {
	return t.allowNull
}

func (t *Integer) setAllowNull(b bool) {
	t.allowNull = b
}
