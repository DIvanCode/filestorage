package queue

import (
	"sync"
)

type (
	Queue[T any] struct {
		mu   sync.Mutex
		head *node[T]
		tail *node[T]
	}

	node[T any] struct {
		value T
		next  *node[T]
	}
)

func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{
		head: nil,
		tail: nil,
	}
}

func (q *Queue[T]) Enqueue(value T) {
	node := &node[T]{value: value, next: nil}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head == nil {
		q.head = node
		q.tail = node
	} else {
		q.tail.next = node
		q.tail = node
	}
}

func (q *Queue[T]) Dequeue() *T {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head == nil {
		return nil
	}

	value := q.head.value
	q.head = q.head.next
	return &value
}
