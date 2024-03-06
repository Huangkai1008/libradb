package util

import (
	"github.com/Huangkai1008/libradb/pkg/typing"
)

// SearchIndex returns the index the item should be into the range.
//
// For example, if items are [1, 3, 5], index 0 represents the range [1, 3),
// index 1 represents the range [3, 5), and index 2 represents the range [5, ∞).
// So, if v is 3, the function will return 1.
// If v is 4, the function will return 1, because 4 is in the range [3, 5).
// If v is 5, the function will return 2.
// If v is less than the first item, the function will return -1.
// Note: The items must be sorted in ascending order.
func SearchIndex[T typing.Comparable[T]](v T, items []T) int {
	if len(items) == 0 || v.Compare(items[0]) < 0 {
		return -1
	}

	// Perform binary search to find the appropriate index
	left, right := 0, len(items)
	for left < right {
		mid := left + (right-left)/2
		if v.Compare(items[mid]) == 0 {
			return mid
		} else if v.Compare(items[mid]) > 0 {
			left = mid
		} else {
			right = mid - 1
		}
	}
	return left
}
