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
	// replacer   *LFUReplacer
	replacer *LRUReplacer

	freeList DoubleList
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

	// manager.replacer = CreateLFUReplacer()
	manager.replacer = CreateLRUReplacer()

	manager.freeList = *CreateDL()

	for i := 0; i < manager.capacity; i++ {
		node := &ListNode{pageId: PageId(i)}
		node.insert(manager.freeList.head)
	}

	return manager
}

func (manager *BufferPoolManager) Get(pageId PageId) *Page {
	return &manager.pages[pageId]
}

func (manager *BufferPoolManager) fetchPage(id BlockId) *Page {
	manager.replacer.locker.Lock()
	// 已经缓存在buffer里了
	if pageId := getPageId(manager.block2page, id); pageId != -1 {
		page := &manager.pages[pageId]
		if page.pin == 0 {
			manager.replacer.erase(pageId)
		}
		atomic.AddInt32(&page.pin, 1)

		manager.replacer.locker.Unlock()
		return page
	}

	// 从free list里取buffer page
	if !manager.freeList.isEmpty() {
		// 从free list上取
		manager.freeList.locker.Lock()
		node := manager.freeList.tail.prev
		node.remove()
		manager.freeList.locker.Unlock()

		// page读取block
		manager.pages[node.pageId].loadPage(id)
		manager.block2page.Store(id, node.pageId)
		manager.replacer.locker.Unlock()

		return &manager.pages[node.pageId]
	}

	// 从replacer里淘汰page出来
	if pageId := manager.replacer.victim(); pageId != -1 {
		page := &manager.pages[pageId]

		manager.block2page.Delete(page.blockId)

		page.loadPage(id)
		manager.block2page.Store(id, page.pageId)
		manager.replacer.locker.Unlock()

		return page
	}
	manager.replacer.locker.Unlock()

	return nil
}

func (manager *BufferPoolManager) drop(id PageId) {
	manager.replacer.locker.Lock()
	atomic.AddInt32(&manager.pages[id].pin, -1)
	if manager.pages[id].pin == 0 {
		manager.replacer.insert(id)
	}
	manager.replacer.locker.Unlock()

}
