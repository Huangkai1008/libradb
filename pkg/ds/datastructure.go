package ds

import dll "github.com/emirpasic/gods/v2/lists/doublylinkedlist"

type LinkedList[T comparable] interface {
	// Append a value at the tail of the list.
	Append(value T)
	// Size of the linked list.
	Size() int
	// Get returns the element at index.
	Get(index int) T
	// Insert element at the given index.
	Insert(index int, value T)
	// Remove the element at the given index from the list and return it.
	Remove(index int) T
}

type DoublyLinkedList[T comparable] struct {
	list *dll.List[T]
}

func NewDLL[T comparable]() *DoublyLinkedList[T] {
	return &DoublyLinkedList[T]{
		list: dll.New[T](),
	}
}

func (d *DoublyLinkedList[T]) Append(value T) {
	d.list.Append(value)
}

func (d *DoublyLinkedList[T]) Size() int {
	return d.list.Size()
}

func (d *DoublyLinkedList[T]) Get(index int) T {
	element, _ := d.list.Get(index)
	return element
}

func (d *DoublyLinkedList[T]) Insert(index int, value T) {
	d.list.Insert(index, value)
}

func (d *DoublyLinkedList[T]) Remove(index int) T {
	element := d.Get(index)
	d.list.Remove(index)
	return element
}
