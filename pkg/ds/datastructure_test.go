package ds_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2" //nolint:revive  // ginkgo
	. "github.com/onsi/gomega"    //nolint:revive  // ginkgo

	"github.com/Huangkai1008/libradb/pkg/ds"
)

var _ = Describe("LinkedList", func() {
	var linkedList ds.LinkedList[int]
	AssertLinkedListBehavior := func() {
		It("should be empty", func() {
			Expect(linkedList.Size()).To(BeZero())
		})

		Specify("append 10 items", func() {
			for i := 0; i < 10; i++ {
				linkedList.Append(i)
			}
			Expect(linkedList.Size()).To(Equal(10))
		})

		Specify("insert item", func() {
			for i := 10; i < 20; i++ {
				linkedList.Append(i)
			}
			linkedList.Insert(2, 99)
		})

		It("can remove element", func() {
			linkedList.Remove(8)
			linkedList.Remove(18)
			linkedList.Remove(3)
			linkedList.Remove(1)
		})

		It("can delete element", func() {
			linkedList.Remove(8)
			linkedList.Remove(18)
			linkedList.Remove(3)
			linkedList.Remove(1)
		})

		Specify("clear all the elements", func() {
			size := linkedList.Size()
			for i := 0; i < size; i++ {
				linkedList.Remove(0)
			}
		})

		It("should not be empty again", func() {
			Expect(linkedList.Size()).To(BeZero())
		})

	}

	Describe("DoublyLinkedList", Ordered, func() {
		BeforeAll(func() {
			linkedList = ds.NewDLL[int]()
		})

		AssertLinkedListBehavior()
	})
})

func TestLinkedList(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data Structure Suite")
}
