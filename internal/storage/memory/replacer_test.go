package memory_test

import (
	"github.com/Huangkai1008/libradb/internal/storage/table"
	. "github.com/onsi/ginkgo/v2" //nolint:revive  // ginkgo
	. "github.com/onsi/gomega"    //nolint:revive  // ginkgo

	"github.com/Huangkai1008/libradb/internal/storage/memory"
)

var _ = Describe("LRUKReplacer", func() {
	k := 2

	var replacer *memory.LRUKReplacer

	Describe("LRUKReplacer test", Ordered, func() {
		BeforeAll(func() {
			replacer = memory.NewLRUKReplacer(k)
		})

		Describe("Evict page from replacer", func() {
			It("should get correct evict page", func() {
				By("adding pages into replacer", func() {
					replacer.Access(1)
					replacer.Access(2)
					replacer.Access(3)
				})

				By("set evictable into the replacer", func() {
					replacer.SetEvictable(1, true)
					replacer.SetEvictable(2, true)
					replacer.SetEvictable(3, true)
				})

				By("set one page access count to k", func() {
					replacer.Access(1)
				})

				pageNumber, err := replacer.Evict()
				Expect(err).NotTo(HaveOccurred())
				Expect(pageNumber).To(Equal(table.PageNumber(2)))
			})

			It("should skip the page which not evictable", func() {
				By("adding pages into replacer", func() {
					replacer.Access(1)
					replacer.Access(2)
					replacer.Access(3)
				})

				By("set evictable into the replacer", func() {
					replacer.SetEvictable(1, true)
					replacer.SetEvictable(2, false)
					replacer.SetEvictable(3, true)
				})

				By("set one page access count to k", func() {
					replacer.Access(1)
				})

				pageNumber, err := replacer.Evict()
				Expect(err).NotTo(HaveOccurred())
				Expect(pageNumber).To(Equal(table.PageNumber(3)))
			})

			It("should raise error if no pages to evict", func() {
				By("adding pages into replacer", func() {
					replacer.Access(1)
					replacer.Access(2)
					replacer.Access(3)
				})

				By("set evictable into the replacer", func() {
					replacer.SetEvictable(1, false)
					replacer.SetEvictable(2, false)
					replacer.SetEvictable(3, false)
				})

				pageNumber, err := replacer.Evict()
				Expect(err).To(HaveOccurred())
				Expect(pageNumber).To(Equal(table.InvalidPageNumber))
			})
		})

		Describe("Remove page from replacer", func() {
			When("page is evictable", func() {
				It("should remove successfully", func() {
					replacer.Access(1)
					replacer.Access(1)
					replacer.Access(1)
					replacer.Access(1)
					replacer.Access(2)
					replacer.SetEvictable(1, true)
					replacer.SetEvictable(2, false)

					pageNumber, err := replacer.Evict()
					Expect(err).NotTo(HaveOccurred())
					Expect(pageNumber).To(Equal(table.PageNumber(1)))

					err = replacer.Remove(pageNumber)
					Expect(err).NotTo(HaveOccurred())
				})
			})
		})
	})
})
