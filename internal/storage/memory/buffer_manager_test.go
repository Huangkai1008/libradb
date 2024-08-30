package memory_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2" //nolint:revive  // ginkgo
	. "github.com/onsi/gomega"    //nolint:revive  // ginkgo

	"github.com/Huangkai1008/libradb/internal/storage/disk"
	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

var _ = Describe("Buffer manager", Ordered, func() {
	poolSize := uint16(5)
	tableSpaceID := table.SpaceID(1)
	var diskManager disk.Manager
	var replacer memory.Replacer
	var bufferManager memory.BufferManager
	var schema *table.Schema

	BeforeAll(func() {
		replacer = memory.NewLRUKReplacer(2)
		diskManager = disk.NewMemoryDiskManager()
		schema = table.NewSchema()
	})

	AssertBufferManagerBehavior := func() {
		BeforeEach(func() {
			bufferManager = memory.NewBufferPool(poolSize, diskManager, replacer)
		})

		AfterEach(func() {
			_ = bufferManager.Close()
		})

		Describe("Apply pages from buffer manager", func() {
			When("pool is empty", func() {
				It("should apply a page successfully", func() {
					p := table.NewDataPage(true)
					err := bufferManager.ApplyNewPage(tableSpaceID, p)
					Expect(err).To(BeNil())
				})
			})

			When("pool is not full", func() {
				It("should apply pages successfully", func() {
					for i := uint16(0); i < poolSize; i++ {
						p := table.NewDataPage(true)
						err := bufferManager.ApplyNewPage(tableSpaceID, p)
						Expect(err).To(BeNil())
					}
				})
			})

			When("pool is full", func() {
				It("should raise an error", func() {
					for i := uint16(0); i < poolSize; i++ {
						p := table.NewDataPage(true)
						_ = bufferManager.ApplyNewPage(tableSpaceID, p)
					}

					p := table.NewDataPage(true)
					err := bufferManager.ApplyNewPage(tableSpaceID, p)
					Expect(err).ToNot(BeNil())
					Expect(err).To(MatchError(memory.ErrBufferPoolIsFull))
				})
			})

			When("unpin unused pages", func() {
				It("should can apply new page", func() {
					By("creating pages to fill the pool")
					pageNumbers := make([]table.PageNumber, poolSize)
					for i := uint16(0); i < poolSize; i++ {
						p := table.NewDataPage(true)
						err := bufferManager.ApplyNewPage(tableSpaceID, p)
						Expect(err).To(BeNil())
						pageNumbers[i] = p.PageNumber()
					}

					By("unpin one page to free")
					bufferManager.Unpin(pageNumbers[0], true)

					By("can create a new page again now")
					p := table.NewDataPage(true)
					err := bufferManager.ApplyNewPage(tableSpaceID, p)
					Expect(err).To(BeNil())
				})
			})
		})

		Describe("Fetch pages from buffer manager", func() {
			When("page is on the pool", func() {
				It("should get page directly", func() {
					p := table.NewDataPage(true)
					pageNumber := p.PageNumber()
					_ = bufferManager.ApplyNewPage(tableSpaceID, p)

					fetchP, err := bufferManager.FetchPage(pageNumber, schema)
					Expect(err).To(BeNil())
					Expect(p.PageNumber()).To(Equal(pageNumber))
					Expect(fetchP.Buffer()).To(Equal(p.Buffer()))
				})
			})

			When("page is not on the pool", func() {
				It("should get page from disk", func() {
					By("creating pages to fill the pool")
					pageNumbers := make([]table.PageNumber, poolSize)
					for i := uint16(0); i < poolSize; i++ {
						p := table.NewDataPage(true)
						err := bufferManager.ApplyNewPage(tableSpaceID, p)
						Expect(err).To(BeNil())
						pageNumbers[i] = p.PageNumber()
					}

					By("unpin one page to free")
					bufferManager.Unpin(pageNumbers[0], true)

					By("create a new page again now")
					p := table.NewDataPage(true)
					applyErr := bufferManager.ApplyNewPage(tableSpaceID, p)
					Expect(applyErr).To(BeNil())
					bufferManager.Unpin(p.PageNumber(), true)

					By("get page from disk")
					fetchP, err := bufferManager.FetchPage(pageNumbers[0], schema)
					Expect(err).To(BeNil())
					Expect(fetchP.PageNumber()).To(Equal(pageNumbers[0]))
				})
			})
		})
	}

	Describe("Buffer pool manager", Ordered, func() {
		AssertBufferManagerBehavior()
	})

})

func TestBufferManager(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Buffer Manager Suite")
}
