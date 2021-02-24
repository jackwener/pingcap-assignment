package buffer

type LRUReplacer struct {
	//	mutex sync.RWMutex

	listHead *ListNode
	listTail *ListNode

	dict map[PageId]*ListNode
}

func CreateReplacer() *LRUReplacer {
	replacer := &LRUReplacer{
		// maxItemSize: maxItemSize,
		listHead: &ListNode{pageId: -1},
		listTail: &ListNode{pageId: -1},
		dict:     make(map[PageId]*ListNode),
	}

	replacer.listTail.prev = replacer.listHead
	replacer.listHead.next = replacer.listTail

	return replacer
}

// 插Head
func (replacer *LRUReplacer) insert(pageId PageId) bool {
	// TODO: 已经存在
	node := &ListNode{pageId: pageId}

	node.insert(replacer.listHead, replacer.listHead.next)

	replacer.dict[pageId] = node

	return true
}

// 取tail
func (replacer *LRUReplacer) victim() PageId {
	if replacer.listHead.next == replacer.listTail {
		return -1
	}

	node := replacer.listTail.prev

	node.remove()

	delete(replacer.dict, node.pageId)

	return node.pageId
}

func (replacer *LRUReplacer) erase(pageId PageId) bool {
	if node, ok := replacer.dict[pageId]; ok {
		node.remove()
		return true
	}

	return false
}

func (replacer *LRUReplacer) use(pageId PageId) bool {
	if node, ok := replacer.dict[pageId]; ok {
		node.remove()
		node.insert(replacer.listHead, replacer.listHead.next)
		return true
	}

	return false
}
