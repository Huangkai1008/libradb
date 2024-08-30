package typing

type Comparable[T any] interface {
	Compare(t T) int
}

type Iterator[T any] interface {
	Next() T
}

type BacktrackingIterator[T any] interface {
	Iterator[T]
	Prev() T
}
