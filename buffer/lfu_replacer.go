package buffer

type LFUReplacer struct {
	findMap  map[PageId]*ListNode
	countMap map[int32]*DoubleList
	size     int32
	minFreq  int32
}

func CreateLFUReplacer() *LFUReplacer {
	return &LFUReplacer{
		findMap:  make(map[PageId]*ListNode),
		countMap: make(map[int32]*DoubleList),
	}
}

//func (replacer *LFUReplacer) Get(pageId PageId) int {
//	if node, ok := replacer.findMap[pageId]; ok {
//		replacer.IncFreq(node)
//		return node.val
//	}
//	return -1
//}

func (replacer *LFUReplacer) insert(pageId PageId) bool {
	if node, ok := replacer.findMap[pageId]; ok {
		replacer.IncFreq(node)
		return true
	}

	node := &ListNode{pageId: pageId, freq: 1}
	replacer.findMap[pageId] = node
	if replacer.countMap[1] == nil {
		replacer.countMap[1] = CreateDL()
	}
	replacer.countMap[1].addFirst(node)
	replacer.minFreq = 1
	replacer.size++

	return true
}

func (replacer *LFUReplacer) IncFreq(node *ListNode) {
	node.remove()
	if replacer.minFreq == node.freq && replacer.countMap[node.freq].isEmpty() {
		replacer.minFreq++
		delete(replacer.countMap, node.freq)
	}

	node.freq++
	if replacer.countMap[node.freq] == nil {
		replacer.countMap[node.freq] = CreateDL()
	}
	replacer.countMap[node.freq].addFirst(node)
}

func (replacer *LFUReplacer) victim() PageId {
	node := replacer.countMap[replacer.minFreq].removeLast()
	delete(replacer.findMap, node.pageId)
	replacer.size--

	return node.pageId
}
