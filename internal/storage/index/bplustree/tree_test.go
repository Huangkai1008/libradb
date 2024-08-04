package bplustree_test

import (
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo/v2" //nolint:revive  // ginkgo
	. "github.com/onsi/gomega"    //nolint:revive  // ginkgo

	"github.com/Huangkai1008/libradb/internal/field"
	"github.com/Huangkai1008/libradb/internal/storage/disk"
	"github.com/Huangkai1008/libradb/internal/storage/index/bplustree"
	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

type DummyReplacer struct {
	accessCounter map[page.Number]int
}

func NewDummyReplacer() *DummyReplacer {
	return &DummyReplacer{
		accessCounter: make(map[page.Number]int),
	}
}

func (d *DummyReplacer) Access(pageNumber page.Number) {
	d.accessCounter[pageNumber]++
}

var _ = Describe("B+ Tree Index", Ordered, func() {
	var schema *table.Schema
	var bufferManager memory.BufferManager
	var tree *bplustree.BPlusTree
	var pkType field.Type

	BeforeAll(func() {
		schema = table.NewSchema().
			WithField("id", field.NewInteger()).
			WithField("name", field.NewVarchar()).
			WithField("age", field.NewInteger()).
			WithField("is_student", field.NewBoolean()).
			WithField("score", field.NewFloat())

		pkType = field.NewInteger()
	})

	BeforeEach(func() {
		replacer := memory.NewLRUKReplacer(5)
		diskManager := disk.NewMemoryDiskManager()
		bufferManager = memory.NewBufferPool(1024, diskManager, replacer)
		DeferCleanup(bufferManager.Close)
	})

	Describe("Put in B+ Tree", func() {
		BeforeEach(func() {
			tree, _ = bplustree.NewBPlusTree(&bplustree.Metadata{
				Order:  1,
				Schema: schema,
			}, bufferManager)
		})
		DescribeTable("Put a key in tree",
			func(key int, values []any) {
				record := page.NewRecordFromLiteral(values...)
				err := tree.Put(field.NewValue(pkType, key), record)
				Expect(err).ToNot(HaveOccurred())
			},
			EntryDescription("put %d with value %v"),
			Entry(nil, 4, []any{4, "Alice", 20, true, 90.5}),
			Entry(nil, 9, []any{9, "Bob", 21, false, 85.5}),
			Entry(nil, 6, []any{6, "Charlie", 22, true, 80.5}),
			Entry(nil, 2, []any{2, "David", 23, false, 75.5}),
			Entry(nil, 10000, []any{10000, "Rita", 28, true, 68.5}),
			Entry(nil, 1, []any{1, "Adam", 23, false, 95.5}),
		)

		When("Put duplicate key", func() {
			It("should raise error", func() {
				By("Add a key")
				record := page.NewRecordFromLiteral(4, "Alice", 20, true, 90.5)
				err := tree.Put(field.NewValue(pkType, 4), record)
				Expect(err).ToNot(HaveOccurred())

				By("Add another key")
				record = page.NewRecordFromLiteral(9, "Bob", 21, false, 85.5)
				err = tree.Put(field.NewValue(pkType, 9), record)
				Expect(err).ToNot(HaveOccurred())

				By("Add duplicate key")
				record = page.NewRecordFromLiteral(4, "Alice", 20, true, 90.5)
				err = tree.Put(field.NewValue(pkType, 4), record)
				Expect(err).Should(MatchError(bplustree.ErrKeyExists))
			})
		})
	})

	Describe("Get in B+ tree", func() {
		BeforeEach(func() {
			tree, _ = bplustree.NewBPlusTree(&bplustree.Metadata{
				Order:  1,
				Schema: schema,
			}, bufferManager)
		})

		DescribeTable("Get a key in tree",
			func(key int, values []any) {
				By("Put a key in tree")
				record := page.NewRecordFromLiteral(values...)
				err := tree.Put(field.NewValue(pkType, key), record)
				Expect(err).ToNot(HaveOccurred())

				By("Get a key in tree")
				record, err = tree.Get(field.NewValue(pkType, key))
				Expect(err).ToNot(HaveOccurred())
				Expect(record).To(Equal(page.NewRecordFromLiteral(values...)))
			},
			EntryDescription("get %d with value %v"),
			Entry(nil, 4, []any{4, "Alice", 20, true, 90.5}),
			Entry(nil, 9, []any{9, "Bob", 21, false, 85.5}),
			Entry(nil, 6, []any{6, "Charlie", 22, true, 80.5}),
			Entry(nil, 2, []any{2, "David", 23, false, 75.5}),
			Entry(nil, 10000, []any{10000, "Rita", 28, true, 68.5}),
			Entry(nil, 1, []any{1, "Adam", 23, false, 95.5}),
		)
	})

	Describe("Delete in B+ tree", func() {
		BeforeEach(func() {
			tree, _ = bplustree.NewBPlusTree(&bplustree.Metadata{
				Order:  1,
				Schema: schema,
			}, bufferManager)
		})

		DescribeTable("Delete a key in tree",
			func(key int, values []any) {
				By("Put a key in tree")
				record := page.NewRecordFromLiteral(values...)
				err := tree.Put(field.NewValue(pkType, key), record)
				Expect(err).ToNot(HaveOccurred())

				By("Delete the key in tree")
				err = tree.Delete(field.NewValue(pkType, key))
				Expect(err).ToNot(HaveOccurred())

				By("Get the key in tree")
				record, err = tree.Get(field.NewValue(pkType, key))
				Expect(err).ToNot(HaveOccurred())
				Expect(record).To(BeNil())
			},
			EntryDescription("remove %d with value %v"),
			Entry(nil, 4, []any{4, "Alice", 20, true, 90.5}),
			Entry(nil, 9, []any{9, "Bob", 21, false, 85.5}),
			Entry(nil, 6, []any{6, "Charlie", 22, true, 80.5}),
			Entry(nil, 2, []any{2, "David", 23, false, 75.5}),
			Entry(nil, 10000, []any{10000, "Rita", 28, true, 68.5}),
			Entry(nil, 1, []any{1, "Adam", 23, false, 95.5}),
		)

		When("Delete non-existing key in tree", func() {
			It("should do nothing", func() {
				By("Put a key in tree")
				record := page.NewRecordFromLiteral()
				err := tree.Put(field.NewValue(pkType, 4), record)
				Expect(err).ToNot(HaveOccurred())

				By("Delete the keys in tree")
				for i := 0; i < 5; i++ {
					err = tree.Delete(field.NewValue(pkType, 4))
					Expect(err).ToNot(HaveOccurred())
				}
			})
		})
	})

	Describe("WhiteBox test", func() {

		BeforeEach(func() {
			tree, _ = bplustree.NewBPlusTree(&bplustree.Metadata{
				Order:  1,
				Schema: schema,
			}, bufferManager)
		})

		It("should succeed", func() {
			By("Puts and gets in tree")
			putBehavior := func(key int, values []any) {
				By(fmt.Sprintf("Put %d in tree", key))
				err := tree.Put(
					field.NewValue(pkType, key),
					page.NewRecordFromLiteral(values...),
				)
				Expect(err).ToNot(HaveOccurred())

				By(fmt.Sprintf("Get %d in tree", key))
				retrievedRecord, err := tree.Get(field.NewValue(pkType, key))
				Expect(err).ToNot(HaveOccurred())
				Expect(retrievedRecord).To(Equal(page.NewRecordFromLiteral(values...)))
				GinkgoWriter.Println(tree)
			}

			// (4)
			putBehavior(4, []any{4, "Alice", 20, true, 90.5})

			// (4, 9)
			putBehavior(9, []any{9, "Bob", 21, false, 85.5})

			//   (6)
			//  /   \
			// (4) (6 9)
			putBehavior(6, []any{6, "Charlie", 22, true, 80.5})

			//     (6)
			//    /   \
			// (2 4) (6 9)
			putBehavior(2, []any{2, "David", 23, false, 75.5})

			//      (6 7)
			//     /  |  \
			// (2 4) (6) (7 9)
			putBehavior(7, []any{7, "Eve", 24, true, 70.5})

			//         (7)
			//        /   \
			//     (6)     (8)
			//    /   \   /   \
			// (2 4) (6) (7) (8 9)
			putBehavior(8, []any{8, "Frank", 25, false, 65.5})

			//            (7)
			//           /   \
			//     (3 6)       (8)
			//   /   |   \    /   \
			// (2) (3 4) (6) (7) (8 9)
			putBehavior(3, []any{3, "Grace", 26, true, 60.5})

			//           (4 7)
			//          /  |  \
			//   (3)      (6)       (8)
			//  /   \    /   \    /   \
			// (2) (3) (4 5) (6) (7) (8 9)
			putBehavior(5, []any{5, "Hank", 27, true, 80.5})

			//            (4 7)
			//           /  |  \
			//    (3)      (6)       (8)
			//   /   \    /   \    /   \
			// (1 2) (3) (4 5) (6) (7) (8 9)
			putBehavior(1, []any{1, "Smith", 68, true, 78})

			By("Deletes and gets in tree")
			deleteBehavior := func(key int) {
				By(fmt.Sprintf("Delete %d in tree", key))
				err := tree.Delete(field.NewValue(pkType, key))
				Expect(err).ToNot(HaveOccurred())

				By(fmt.Sprintf("Get %d in tree", key))
				retrievedRecord, err := tree.Get(field.NewValue(pkType, key))
				Expect(err).ToNot(HaveOccurred())
				Expect(retrievedRecord).To(BeNil())
				GinkgoWriter.Println(tree)
			}

			//            (4 7)
			//           /  |  \
			//    (3)      (6)       (8)
			//   /   \    /   \    /   \
			// (  2) (3) (4 5) (6) (7) (8 9)
			deleteBehavior(1)

			//            (4 7)
			//           /  |  \
			//    (3)      (6)       (8)
			//   /   \    /   \    /   \
			// (  2) (3) (4 5) (6) (7) (8  )
			deleteBehavior(9)

			//            (4 7)
			//           /  |  \
			//    (3)      (6)       (8)
			//   /   \    /   \    /   \
			// (  2) (3) (4 5) ( ) (7) (8  )
			deleteBehavior(6)

			//            (4 7)
			//           /  |  \
			//    (3)      (6)       (8)
			//   /   \    /   \    /   \
			// (  2) (3) (  5) ( ) (7) (8  )
			deleteBehavior(4)

			//            (4 7)
			//           /  |  \
			//    (3)      (6)       (8)
			//   /   \    /   \    /   \
			// (   ) (3) (  5) ( ) (7) (8  )
			deleteBehavior(2)

			//            (4 7)
			//           /  |  \
			//    (3)      (6)       (8)
			//   /   \    /   \    /   \
			// (   ) (3) (   ) ( ) (7) (8  )
			deleteBehavior(5)

			//            (4 7)
			//           /  |  \
			//    (3)      (6)       (8)
			//   /   \    /   \    /   \
			// (   ) (3) (   ) ( ) ( ) (8  )
			deleteBehavior(7)

			//            (4 7)
			//           /  |  \
			//    (3)      (6)       (8)
			//   /   \    /   \    /   \
			// (   ) ( ) (   ) ( ) ( ) (8  )
			deleteBehavior(3)

			//            (4 7)
			//           /  |  \
			//    (3)      (6)       (8)
			//   /   \    /   \    /   \
			// (   ) ( ) (   ) ( ) ( ) (   )
			deleteBehavior(8)
		})
	})
})

func TestBPlusTree(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "B+ Tree Suite")
}
