package buffer

import (
	"log"
	"sync"
	"sync/atomic"
)

type BufferPoolManager struct {
	capacity int
	pages    []Page
	//	pageTable  map[PageId]Page

	// block2page map[BlockId]PageId
	block2page *sync.Map
	replacer   *LRUReplacer

	freeList FreeList
}

type FreeList struct {
	freeListHead *ListNode // dummy
	freeListTail *ListNode // dummy
	listLen      int32

	locker *sync.Mutex
}

func getPageId(m *sync.Map, blockId BlockId) PageId {
	if v, ok := m.Load(blockId); ok {
		if pageId, ok := v.(PageId); ok {
			return pageId
		}

		log.Println("type error")
		return -1
	}
	return -1
}

// 头尾dummy node的双向链表
type ListNode struct {
	pageId     PageId
	next, prev *ListNode
	// locker     *sync.Mutex
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

func CreateBufferPoolManager() *BufferPoolManager {
	manager := &BufferPoolManager{
		pages:    make([]Page, PageNum),
		capacity: PageNum,
		//block2page: make(map[BlockId]PageId, PageNum),
	}

	var m sync.Map
	var locker sync.Mutex
	manager.block2page = &m
	manager.freeList.locker = &locker

	for i := 0; i < PageNum; i++ {
		manager.pages[i].initPage(PageId(i))
	}

	manager.replacer = CreateReplacer()

	manager.freeList.freeListHead = &ListNode{pageId: -1}
	manager.freeList.freeListTail = &ListNode{pageId: -1}

	manager.freeList.freeListHead.next = manager.freeList.freeListTail
	manager.freeList.freeListTail.prev = manager.freeList.freeListHead

	for i := 0; i < manager.capacity; i++ {
		node := &ListNode{pageId: PageId(i)}
		node.insert(manager.freeList.freeListHead)

		manager.freeList.listLen += 1
	}

	return manager
}

func (manager *BufferPoolManager) Get(pageId PageId) *Page {
	return &manager.pages[pageId]
}

func (manager *BufferPoolManager) fetchPage(id BlockId) *Page {
	// 已经缓存在buffer里了
	if pageId := getPageId(manager.block2page, id); pageId != -1 {
		page := &manager.pages[pageId]
		atomic.AddInt32(&page.pin, 1)

		return page
	}

	// 从free list里取buffer page
	if manager.freeList.freeListHead.next != manager.freeList.freeListTail {
		// 从free list上取
		manager.freeList.locker.Lock()
		node := manager.freeList.freeListTail.prev
		node.remove()
		manager.freeList.locker.Unlock()
		atomic.AddInt32(&manager.freeList.listLen, -1)

		// pin 该页
		atomic.AddInt32(&manager.pages[node.pageId].pin, 1)

		// page读取block
		manager.pages[node.pageId].loadPage(id)
		manager.block2page.Store(id, node.pageId)

		return &manager.pages[node.pageId]
	}

	// 从replacer里淘汰page出来
	if pageId := manager.replacer.victim(); pageId != -1 {
		page := &manager.pages[pageId]
		atomic.AddInt32(&page.pin, 1)
		manager.block2page.Delete(page.blockId)

		page.loadPage(id)
		manager.block2page.Store(id, page.pageId)

		return page
	}

	return nil
}
