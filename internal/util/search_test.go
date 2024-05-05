package util_test

import (
	"cmp"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Huangkai1008/libradb/internal/util"
)

type S struct {
	value int
}

func (s S) Compare(t S) int {
	return cmp.Compare[int](s.value, t.value)
}

func (s S) String() string {
	return strconv.Itoa(s.value)
}

func TestSearchIndex(t *testing.T) {
	var tests = []struct {
		v        S
		items    []S
		expected int
	}{
		{S{20}, []S{{1}, {5}, {12}, {200}}, 2},
		{S{27}, []S{{1}, {320}, {360}}, 0},
		{S{320}, []S{{1}, {320}, {360}}, 1},
		{S{1}, []S{{1}, {320}, {360}}, 0},
		{S{0}, []S{{1}, {320}, {360}}, -1},
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
		v        S
		items    []S
		expected int
	}{
		{S{20}, []S{{1}, {5}, {12}, {200}}, 3},
		{S{27}, []S{{1}, {320}, {360}}, 1},
		{S{320}, []S{{1}, {320}, {360}}, 1},
		{S{1}, []S{{1}, {320}, {360}}, 0},
		{S{0}, []S{{1}, {320}, {360}}, 0},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			actual := util.InsertIndex(tt.v, tt.items)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
