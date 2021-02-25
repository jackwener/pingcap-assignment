package buffer

import (
	"log"
	"sync"
)

type LRUReplacer struct {
	list DoubleList
	// dict map[PageId]*ListNode
	dict *sync.Map

	locker *sync.Mutex
}

func CreateLRUReplacer() *LRUReplacer {
	replacer := &LRUReplacer{
		list: *CreateDL(),
	}
	var m sync.Map
	var locker sync.Mutex

	replacer.locker = &locker
	replacer.dict = &m

	replacer.list.tail.prev = replacer.list.head
	replacer.list.head.next = replacer.list.tail

	return replacer
}

// 插Head
func (replacer *LRUReplacer) insert(pageId PageId) bool {
	// 场景中不可能find
	//if node := getListNode(replacer.dict, int32(pageId)); node != nil {
	//	replacer.list.locker.Lock()
	//	node.remove()
	//	node.insert(replacer.list.head)
	//	replacer.list.locker.Unlock()
	//	return true
	//}

	node := &ListNode{pageId: pageId}

	node.insert(replacer.list.head)

	replacer.dict.Store(pageId, node)

	return true
}

// 取tail
func (replacer *LRUReplacer) victim() PageId {

	if replacer.list.isEmpty() {
		return -1
	}

	node := replacer.list.tail.prev
	node.remove()

	replacer.dict.Delete(node.pageId)

	return node.pageId
}

func (replacer *LRUReplacer) erase(pageId PageId) bool {
	if node := getListNode(replacer.dict, int32(pageId)); node != nil {

		node.remove()

		return true
	}

	return false
}

func getListNode(m *sync.Map, pageId int32) *ListNode {
	if v, ok := m.Load(pageId); ok {
		if node, ok := v.(*ListNode); ok {
			return node
		}

		log.Println("type error")
		return nil
	}
	return nil
}
