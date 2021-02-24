package buffer

type BufferPoolManager struct {
	capacity int
	pages    []Page
	//	pageTable  map[PageId]Page

	block2page map[BlockId]PageId
	// block2page sync.Map
	replacer *LRUReplacer

	freeListHead *ListNode // dummy
	freeListTail *ListNode // dummy
	listLen      int
}

type ListNode struct {
	pageId     PageId
	next, prev *ListNode
}

func (node *ListNode) remove() {
	node.prev.next = node.next
	node.next.prev = node.prev

	node.next = nil
	node.prev = nil
}

func (node *ListNode) insert(prev *ListNode, next *ListNode) {
	prev.next = node
	next.prev = node

	node.prev = prev
	node.next = next
}

func CreateBufferPoolManager() *BufferPoolManager {
	manager := &BufferPoolManager{
		pages:      make([]Page, PageNum),
		capacity:   PageNum,
		block2page: make(map[BlockId]PageId, PageNum),
	}

	for i := 0; i < PageNum; i++ {
		manager.pages[i].initPage(PageId(i))
	}

	manager.replacer = CreateReplacer()

	manager.freeListHead = &ListNode{pageId: -1}
	manager.freeListTail = &ListNode{pageId: -1}

	manager.freeListHead.next = manager.freeListTail
	manager.freeListTail.prev = manager.freeListHead

	for i := 0; i < manager.capacity; i++ {
		node := &ListNode{pageId: PageId(i)}
		node.insert(manager.freeListHead, manager.freeListHead.next)

		manager.listLen += 1
	}

	return manager
}

func (manager *BufferPoolManager) Get(pageId PageId) *Page {
	return &manager.pages[pageId]
}

func (manager *BufferPoolManager) fetchPage(id BlockId) *Page {
	//if v, ok := manager.block2page.Load(id); ok {
	//	if pageId, ok := v.(PageId); ok {
	//		page := manager.pages[int(pageId)]
	//		return &page
	//	}
	//
	//	log.Println("type error")
	//	return nil
	//}
	// 已经缓存在buffer里了
	if pageId, ok := manager.block2page[id]; ok {
		manager.replacer.use(pageId)
		return &manager.pages[pageId]
	}

	// 从free list里取buffer page
	if manager.freeListHead.next != manager.freeListTail {
		// 从free list上取
		node := manager.freeListTail.prev

		node.remove()
		manager.listLen -= 1

		// map记录
		manager.block2page[id] = node.pageId

		// 加入replacer
		manager.replacer.insert(node.pageId)

		// page读取block
		manager.pages[node.pageId].loadPage(id)

		return &manager.pages[node.pageId]
	}

	// 从replacer里淘汰page出来
	if pageId := manager.replacer.victim(); pageId != -1 {
		page := &manager.pages[pageId]
		delete(manager.block2page, page.blockId)

		page.loadPage(id)
		manager.replacer.insert(page.pageId)
		manager.block2page[id] = pageId

		return page
	}

	return nil
}

func dropPage(id PageId) {

}
