package disk_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2" //nolint:revive  // ginkgo
	. "github.com/onsi/gomega"    //nolint:revive  // ginkgo

	"github.com/Huangkai1008/libradb/internal/config"
	"github.com/Huangkai1008/libradb/internal/storage/disk"
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

var _ = Describe("Disk space manager", func() {
	var diskManager disk.Manager
	AssertSpaceManagerBehavior := func() {
		Describe("Read/Write page from space manager", func() {
			When("read invalid page", func() {
				It("should return an error", func() {
					pageContent := make([]byte, config.PageSize)
					err := diskManager.ReadPage(table.InvalidPageNumber, pageContent)
					Expect(err).To(HaveOccurred())
					Expect(err).Should(MatchError(disk.ErrPageNotAllocated))
				})
			})

			When("read non-existing page", func() {
				It("should return an error", func() {
					pageContent := make([]byte, config.PageSize)
					err := diskManager.ReadPage(table.InvalidPageNumber, pageContent)
					Expect(err).To(HaveOccurred())
					Expect(err).Should(MatchError(disk.ErrPageNotAllocated))
				})
			})

			When("read write page", func() {
				It("should content-match", func() {
					p := table.NewDataPage(true)
					contents := p.Buffer()

					err := diskManager.WritePage(p.PageNumber(), contents)
					Expect(err).NotTo(HaveOccurred())

					pageContent := make([]byte, config.PageSize)
					err = diskManager.ReadPage(p.PageNumber(), pageContent)
					Expect(err).NotTo(HaveOccurred())
					Expect(pageContent).To(Equal(contents))
				})
			})
		})
	}

	Describe("Memory space manager", Ordered, func() {
		BeforeAll(func() {
			diskManager = disk.NewMemoryDiskManager()
		})

		AfterAll(func() {
			_ = diskManager.Close()
		})

		AssertSpaceManagerBehavior()
	})

	Describe("Disk space manager", Ordered, func() {
		BeforeAll(func() {
			diskManager, _ = disk.NewSpaceManager("/tmp")
		})

		AfterAll(func() {
			_ = diskManager.Close()
		})

		AssertSpaceManagerBehavior()
	})

})

func TestDiskSpaceManager(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Disk Space Manager Suite")
}
