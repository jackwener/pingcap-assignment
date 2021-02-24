package buffer

import (
	"log"
	"sync"
)

type LRUReplacer struct {
	locker *sync.Mutex

	listHead *ListNode
	listTail *ListNode

	dict *sync.Map
	// dict map[PageId]*ListNode
}

func CreateReplacer() *LRUReplacer {
	replacer := &LRUReplacer{
		listHead: &ListNode{pageId: -1},
		listTail: &ListNode{pageId: -1},
		//dict:     make(map[PageId]*ListNode),
	}
	var m sync.Map
	var lock sync.Mutex
	replacer.dict = &m
	replacer.locker = &lock

	replacer.listTail.prev = replacer.listHead
	replacer.listHead.next = replacer.listTail

	return replacer
}

func getListNode(m *sync.Map, pageId PageId) *ListNode {
	if v, ok := m.Load(pageId); ok {
		if node, ok := v.(*ListNode); ok {
			return node
		}

		log.Println("type error")
		return nil
	}
	return nil
}

// 插Head
func (replacer *LRUReplacer) insert(pageId PageId) bool {
	// 已经存在
	if node := getListNode(replacer.dict, pageId); node != nil {
		replacer.locker.Lock()
		node.remove()
		node.insert(replacer.listHead)
		replacer.locker.Unlock()
		return true
	}

	node := &ListNode{pageId: pageId}

	replacer.locker.Lock()
	node.insert(replacer.listHead)
	replacer.dict.Store(pageId, node)
	replacer.locker.Unlock()

	return true
}

// 取tail
func (replacer *LRUReplacer) victim() PageId {
	replacer.locker.Lock()

	if replacer.listHead.next == replacer.listTail {
		return -1
	}

	node := replacer.listTail.prev

	node.remove()
	replacer.locker.Unlock()

	replacer.dict.Delete(node.pageId)

	return node.pageId
}

func (replacer *LRUReplacer) erase(pageId PageId) bool {
	if node := getListNode(replacer.dict, pageId); node != nil {
		node.remove()
		return true
	}

	return false
}
