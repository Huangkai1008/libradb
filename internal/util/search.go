package util

import (
	"sort"

	"github.com/Huangkai1008/libradb/pkg/typing"
)

// SearchIndex returns the index the item should be into the range.
//
// Note: The items must be sorted in ascending order.
func SearchIndex[T typing.Comparable[T]](v T, items []T) int {
	return sort.Search(len(items), func(i int) bool {
		return v.Compare(items[i]) < 0
	})
}

// InsertIndex returns the index the item should be inserted into the items.
//
// Note: The items must be sorted in ascending order.
func InsertIndex[T typing.Comparable[T]](v T, items []T) int {
	left, right := 0, len(items)
	for left < right {
		mid := left + (right-left)/2 //nolint:mnd // avoid overflow
		switch cmp := v.Compare(items[mid]); {
		case cmp == 0:
			return mid
		case cmp > 0:
			left = mid + 1
		default:
			right = mid
		}
	}
	return left
}
