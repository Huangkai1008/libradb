package typing

type Comparable[T any] interface {
	Compare(t T) int
}
