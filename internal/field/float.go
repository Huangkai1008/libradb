package field

type Float struct {
	allowNull bool
}

func NewFloat(options ...Option[*Float]) *Float {
	t := &Float{}
	applyOptions(t, options...)
	return t
}

func (t *Float) TypeID() TypeID {
	return FLOAT
}

func (t *Float) ByteSize() int {
	return 8
}

func (t *Float) Validate() {
}

func (t *Float) AllowNull() bool {
	return t.allowNull
}

func (t *Float) setAllowNull(b bool) {
	t.allowNull = b
}
