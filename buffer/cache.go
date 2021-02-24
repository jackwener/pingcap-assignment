package buffer

import (
	"strings"
)

//type CacheStatus struct {
//	MaxItemSize int
//	CurrentSize int
//}
//
//func (c *MemCache) Status() *CacheStatus {
//	c.mutex.RLock()
//	defer c.mutex.RUnlock()
//	return &CacheStatus{
//		MaxItemSize: c.maxItemSize,
//		CurrentSize: c.cacheList.Len(),
//	}
//}

type MemCache struct {
	//mutex       sync.RWMutex
	//maxItemSize int
	//cacheList   *list.List
	//cache       map[PageId]*list.Element
	blockFirstKey []string
	bufferPoolManager BufferPoolManager
}

func (c *MemCache) Get(key string) (string, error) {
	blockId := c.GetBlockId(key)
	page := c.bufferPoolManager.fetchPage(blockId)
	return page.Store.get(key)
}

func (c *MemCache) GetBlockId(key string) BlockId {
	left := 0
	right := len(c.blockFirstKey) - 1
	var mid int
	for {
		if left > right {
			break
		}

		mid = left + (right-left)/2
		if strings.Compare(key, c.blockFirstKey[mid]) > 0 {
			left = mid + 1
		} else if strings.Compare(key, c.blockFirstKey[mid]) < 0 {
			right = mid - 1
		} else {
			return BlockId(mid)
		}
	}

	return -1
}

