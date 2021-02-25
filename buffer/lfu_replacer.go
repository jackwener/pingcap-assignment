package buffer

import (
	"log"
	"sync"
)

type LFUReplacer struct {
	//findMap  map[PageId]*ListNode
	//countMap map[int32]*DoubleList
	findMap  *sync.Map
	countMap *sync.Map

	size    int32
	minFreq int32

	locker *sync.Mutex
}

func CreateLFUReplacer() *LFUReplacer {
	var locker sync.Mutex
	var fm, cm sync.Map
	return &LFUReplacer{
		findMap:  &fm,
		countMap: &cm,
		locker:   &locker,
	}
}

func (replacer *LFUReplacer) insert(pageId PageId) bool {
	// 不可能find到
	//if node, ok := replacer.findMap[pageId]; ok {
	//	replacer.IncFreq(node)
	//	return true
	//}

	node := &ListNode{pageId: pageId, freq: 1}
	replacer.findMap.Store(pageId, node)
	list := getList(replacer.countMap, 1)
	if list == nil {
		list = CreateDL()
		replacer.countMap.Store(int32(1), CreateDL())
		list.addFirst(node)
	} else {
		list.addFirst(node)
	}

	replacer.minFreq = 1
	replacer.size++

	return true
}

func (replacer *LFUReplacer) IncFreq(node *ListNode) {
	node.remove()
	if replacer.minFreq == node.freq && getList(replacer.countMap, node.freq).isEmpty() {
		replacer.minFreq++
		replacer.countMap.Delete(node.freq)
	}

	node.freq++
	if getList(replacer.countMap, node.freq) == nil {
		replacer.countMap.Store(node.freq, CreateDL())
	}
	getList(replacer.countMap, node.freq).addFirst(node)
}

func (replacer *LFUReplacer) victim() PageId {
	node := getList(replacer.countMap, replacer.minFreq).removeLast()
	replacer.findMap.Delete(node.pageId)
	replacer.size--

	return node.pageId
}

func (replacer *LFUReplacer) erase(pageId PageId) bool {
	if node := getListNode(replacer.findMap, int32(pageId)); node != nil {
		node.remove()
		return true
	}
	return false
}

func (replacer *LFUReplacer) get(pageId PageId) *ListNode {
	if node := getListNode(replacer.findMap, int32(pageId)); node != nil {
		replacer.IncFreq(node)
		return node
	}

	return nil
}

func getList(m *sync.Map, key int32) *DoubleList {
	if v, ok := m.Load(key); ok {
		if node, ok := v.(*DoubleList); ok {
			return node
		}

		log.Println("type error")
		return nil
	}
	return nil
}
