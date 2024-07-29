package bplustree_test

import (
	"errors"
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo/v2" //nolint:revive  // ginkgo
	. "github.com/onsi/gomega"    //nolint:revive  // ginkgo

	"github.com/Huangkai1008/libradb/internal/field"
	"github.com/Huangkai1008/libradb/internal/storage/index/bplustree"
	"github.com/Huangkai1008/libradb/internal/storage/memory"
	"github.com/Huangkai1008/libradb/internal/storage/page"
	"github.com/Huangkai1008/libradb/internal/storage/table"
)

// dummyBufferManager is a dummy implementation of memory.BufferManager.
// Use spaceID and pageNumber as the key to store pages.
type dummyBufferManager struct {
	pageMap map[string]page.Page
}

func newDummyBufferManager() *dummyBufferManager {
	return &dummyBufferManager{
		pageMap: make(map[string]page.Page),
	}
}

func (m *dummyBufferManager) ApplyNewPage(spaceID table.SpaceID, p page.Page) error {
	key := fmt.Sprintf("%d:%d", spaceID, p.PageNumber())
	m.pageMap[key] = p
	return nil
}

func (m *dummyBufferManager) FetchPage(spaceID table.SpaceID, pageNumber page.Number) (page.Page, error) {
	key := fmt.Sprintf("%d:%d", spaceID, pageNumber)
	p, ok := m.pageMap[key]
	if !ok {
		return nil, errors.New("page not found")
	}
	return p, nil
}

//nolint:revive,nilnil // Ignore linter error for now.
func (m *dummyBufferManager) PinPage(spaceID table.SpaceID, pageNumber page.Number) (page.Page, error) {
	return nil, nil
}

//nolint:revive // Ignore linter error for now.
func (m *dummyBufferManager) UnpinPage(spaceID table.SpaceID, pageNumber page.Number) error {
	return nil
}

func (m *dummyBufferManager) Close() error {
	m.pageMap = make(map[string]page.Page)
	return nil
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
		bufferManager = newDummyBufferManager()
		tree, _ = bplustree.NewBPlusTree(&bplustree.Metadata{
			Order:  1,
			Schema: schema,
		}, bufferManager)
	})

	AfterEach(func() {
		_ = bufferManager.Close()
	})

	Describe("Put and Get in B+ Tree", func() {
		It("should put and get records without error", func() {
			records := []struct {
				key           int
				value         *page.Record
				treeStructure string
			}{
				{4, page.NewRecordFromLiteral(4, "Alice", 20, true, 90.5), `
				// (4)
				`},
				{9, page.NewRecordFromLiteral(9, "Bob", 21, false, 85.5), `
				// (4, 9)
				`},
				{6, page.NewRecordFromLiteral(6, "Charlie", 22, true, 80.5), `
				//   (6)
				//  /   \
				// (4) (6 9)
				`},
				{2, page.NewRecordFromLiteral(2, "David", 23, false, 75.5), `
				//     (6)
				//    /   \
				// (2 4) (6 9)
				`},
				{7, page.NewRecordFromLiteral(7, "Eve", 24, true, 70.5), `
				//      (6 7)
				//     /  |  \
				// (2 4) (6) (7 9)
				`},
				{8, page.NewRecordFromLiteral(8, "Frank", 25, false, 65.5), `
				//         (7)
				//        /   \
				//     (6)     (8)
				//    /   \   /   \
				// (2 4) (6) (7) (8 9)
				`},
				{3, page.NewRecordFromLiteral(3, "Grace", 26, true, 60.5), `
				//            (7)
				//           /   \
				//     (3 6)       (8)
				//   /   |   \    /   \
				// (2) (3 4) (6) (7) (8 9)
				`},
				{5, page.NewRecordFromLiteral(5, "Hank", 27, true, 80.5), `
				//            (4 7)
				//           /  |  \
				//   (3)      (6)       (8)
				//  /   \    /   \    /   \
				// (2) (3) (4 5) (6) (7) (8 9)
				`},
				{1, page.NewRecordFromLiteral(1, "Smith", 68, true, 78), `
				//            (4 7)
				//           /  |  \
				//    (3)      (6)       (8)
				//   /   \    /   \    /   \
				// (1 2) (3) (4 5) (6) (7) (8 9)
				`},
			}

			for _, rec := range records {
				By(fmt.Sprintf("Inserting key %d", rec.key), func() {
					err := tree.Put(field.NewValue(pkType, rec.key), rec.value)
					Expect(err).ToNot(HaveOccurred())
					GinkgoWriter.Println(rec.treeStructure)
					GinkgoWriter.Println(tree)
				})

				By(fmt.Sprintf("Getting key %d", rec.key), func() {
					storedRecord, err := tree.Get(field.NewValue(pkType, rec.key))
					Expect(err).ToNot(HaveOccurred())
					Expect(storedRecord).To(Equal(rec.value))
					GinkgoWriter.Println(tree)
				})
			}
		})
	})
})

func TestBPlusTree(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "B+ Tree Suite")
}
