package buffer

type BufferPoolManager struct {
	pages []Page

	block2page map[BlockId]PageId
	pageTable  map[PageId]Page
	replacer   LRUReplacer

	freeListHead *ListNode // dummy
	freeListTail *ListNode // dummy
}

type ListNode struct {
	page       Page
	next, prev *ListNode
}

//func (manager BufferPoolManager) Get(pageId PageId) (Page, error) {
//	page, exists := pagePool.pool[pageId]
//	if exists == false {
//		return Page{}, errors.New("not found")
//	}
//
//	return page, nil
//}
//
//func (manager BufferPoolManager) Put(pageId PageId, page Page) {
//	pagePool.pool[pageId] = page
//}
//
//func (manager BufferPoolManager) Remove(pageId PageId) {
//	delete(pagePool.pool, pageId)
//}

func (manager BufferPoolManager) fetchPage(id BlockId) *Page {
	if pageId, ok := manager.block2page[id]; ok {
		page := manager.pageTable[pageId]
		return &page
	}

	if manager.freeListHead.next != manager.freeListTail {
		node := manager.freeListTail
		manager.freeListTail = manager.freeListTail.prev

		manager.freeListTail.next = nil
		node.prev = nil

		return &node.page
	}

	if page := manager.replacer.Victim(); page != nil {

		return page
	}

	return nil
}

func dropPage(id PageId) {

}
