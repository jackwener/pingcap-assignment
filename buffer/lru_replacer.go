package buffer

type LRUReplacer struct {
	//	mutex sync.RWMutex

	listHead *ListNode
	listTail *ListNode

	dict map[PageId]*ListNode
}

func NewReplacer() *LRUReplacer {
	return &LRUReplacer{
		// maxItemSize: maxItemSize,
		listHead: &ListNode{
			page: Page{
				PageId: -1,
			}},
		listTail: &ListNode{
			page: Page{
				PageId: -1,
			}},
		dict: make(map[PageId]*ListNode),
	}
}

func (replacer *LRUReplacer) Insert(page *Page) bool {
	// TODO: 已经存在

	node := &ListNode{
		page: *page,
	}
	replacer.listHead.next.prev = node
	replacer.listTail.next = node

	replacer.dict[page.PageId] = node

	return true
}

func (replacer *LRUReplacer) Victim() *Page {
	if replacer.listHead == replacer.listTail {
		return nil
	}

	node := replacer.listTail.prev
	node.prev.next = replacer.listTail
	replacer.listTail.prev = node.prev

	node.next = nil
	node.prev = nil

	delete(replacer.dict, node.page.PageId)

	return &node.page
}

func (replacer *LRUReplacer) Erase(pageId PageId) bool {
	if node, ok := replacer.dict[pageId]; ok {
		node.prev.next = node.next
		node.next.prev = node.prev

		node.next = nil
		node.prev = nil
		return true
	}

	return false
}
