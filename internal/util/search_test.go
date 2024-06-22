package util_test

import (
	"cmp"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Huangkai1008/libradb/internal/util"
)

type test struct {
	value int
}

func testsSlice(numbers ...[]int) []test {
	var tests []test
	for _, nums := range numbers {
		for _, num := range nums {
			tests = append(tests, test{num})
		}
	}
	return tests
}

func (s test) Compare(t test) int {
	return cmp.Compare[int](s.value, t.value)
}

func (s test) String() string {
	return strconv.Itoa(s.value)
}

func TestSearchIndex(t *testing.T) {
	var tests = []struct {
		v        test
		items    []test
		expected int
	}{
		{test{5}, testsSlice([]int{25, 50, 60, 75}), 0},
		{test{25}, testsSlice([]int{25, 50, 60, 75}), 1},
		{test{28}, testsSlice([]int{25, 50, 60, 75}), 1},
		{test{50}, testsSlice([]int{25, 50, 60, 75}), 2},
		{test{55}, testsSlice([]int{25, 50, 60, 75}), 2},
		{test{60}, testsSlice([]int{25, 50, 60, 75}), 3},
		{test{75}, testsSlice([]int{25, 50, 60, 75}), 4},
		{test{90}, testsSlice([]int{25, 50, 60, 75}), 4},
		{test{100}, testsSlice([]int{25, 50, 60, 75}), 4},
		{test{0}, testsSlice([]int{25, 50, 60, 75}), 0},
		{test{-1}, testsSlice([]int{25, 50, 60, 75}), 0},
		{test{10000}, testsSlice([]int{25, 50, 60, 75}), 4},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			actual := util.SearchIndex(tt.v, tt.items)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestInsertIndex(t *testing.T) {
	var tests = []struct {
		v        test
		items    []test
		expected int
	}{
		{test{20}, testsSlice([]int{1, 5, 12, 200}), 3},
		{test{27}, testsSlice([]int{1, 320, 360}), 1},
		{test{320}, testsSlice([]int{1, 320, 360}), 1},
		{test{1}, testsSlice([]int{1, 320, 360}), 0},
		{test{0}, testsSlice([]int{1, 320, 360}), 0},
		{test{360}, testsSlice([]int{1, 320, 360}), 2},
		{test{28}, testsSlice([]int{5, 10, 15, 20, 25, 30}), 5},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			actual := util.InsertIndex(tt.v, tt.items)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
