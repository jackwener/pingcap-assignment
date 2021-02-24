package buffer

import "sync"

type DoubleList struct {
	head    *ListNode // dummy
	tail    *ListNode // dummy
	listLen int32

	locker *sync.Mutex
}

// 头尾dummy node的双向链表
type ListNode struct {
	pageId     PageId
	next, prev *ListNode

	freq int32

	// 细粒度锁
	// locker     *sync.Mutex
}

func CreateDL() *DoubleList {
	var locker sync.Mutex
	head, tail := &ListNode{pageId: -1}, &ListNode{pageId: -1}
	head.next, tail.prev = tail, head
	return &DoubleList{
		head:    head,
		tail:    tail,
		listLen: 0,
		locker:  &locker,
	}
}

// 双向链表移除节点
func (node *ListNode) remove() {
	node.prev.next = node.next
	node.next.prev = node.prev

	node.next = nil
	node.prev = nil
}

// 双向链表添加节点
func (node *ListNode) insert(prev *ListNode) {
	node.next = prev.next
	prev.next.prev = node

	prev.next = node
	node.prev = prev
}

func (list *DoubleList) addFirst(node *ListNode) {
	node.insert(list.head)
}

func (list *DoubleList) removeLast() *ListNode {
	if list.isEmpty() {
		return nil
	}

	last := list.tail.prev
	last.remove()

	return last
}

func (list *DoubleList) isEmpty() bool {
	return list.head.next == list.tail
}
