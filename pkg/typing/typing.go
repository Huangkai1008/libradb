package typing

type Comparable[T any] interface {
	Compare(t T) int
}

type Iterator[T any] interface {
	Prev() T
	Next() T
}
