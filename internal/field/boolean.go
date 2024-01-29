package field

type Boolean struct {
	allowNull bool
}

func NewBoolean(options ...Option[*Boolean]) *Boolean {
	t := &Boolean{}
	applyOptions(t, options...)
	return t
}

func (t *Boolean) TypeID() TypeID {
	return BOOLEAN
}

func (t *Boolean) ByteSize() int {
	return 1
}

func (t *Boolean) Validate() {
}

func (t *Boolean) AllowNull() bool {
	return t.allowNull
}

func (t *Boolean) setAllowNull(b bool) {
	t.allowNull = b
}
