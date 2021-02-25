package buffer

type Replacer interface {
	insert(pageId PageId) bool
	victim() PageId
	erase(id PageId) bool
}
