package kvpaxos

import "sync"

type queuenode struct {
	data *Op
	next *queuenode
}

type Queue struct {
	head  *queuenode
	tail  *queuenode
	count int
	lock  *sync.Mutex
}

func NewQueue() *Queue {
	q := &Queue{}
	q.lock = &sync.Mutex{}
	return q
}

func (q *Queue) Len() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.count
}

func (q *Queue) Push(item *Op{}) {
	q.lock.Lock()
	defer q.lock.Unlock()

	n := &queuenode{data: item}

	if q.tail == nil {
		q.tail = n
		q.head = n
	} else {
		q.tail.next = n
		q.tail = n
	}
	q.count++
}

func (q *Queue) Poll() *Op{} {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.head == nil {
		return nil
	}

	n := q.head
	q.head = n.next

	if q.head == nil {
		q.tail = nil
	}
	q.count--

	return n.data
}

func (q *Queue) Peek() *Op{} {
	q.lock.Lock()
	defer q.lock.Unlock()

	n := q.head
	if n == nil || n.data == nil {
		return nil
	}

	return n.data
}
