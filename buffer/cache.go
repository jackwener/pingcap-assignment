package buffer

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
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
	blockFirstKey     []string
	bufferPoolManager *BufferPoolManager
}

func CreateMemCache(blockFirstKey []string) *MemCache {
	return &MemCache{
		blockFirstKey:     blockFirstKey,
		bufferPoolManager: CreateBufferPoolManager(),
	}
}

func (c *MemCache) Get(key string) (string, error) {
	if key == "1163" {
		fmt.Println("debug")
	}

	blockId := c.GetBlockId(key)
	if blockId == -1 {
		return "", errors.New("not found block")
	}
	page := c.bufferPoolManager.fetchPage(blockId)
	if page == nil {
		fmt.Println("get failed")
		return "", errors.New("get failed")
	}
	value, err := page.store.get(key)
	atomic.AddInt32(&page.pin, -1)
	if page.pin == 0 {
		c.bufferPoolManager.replacer.insert(page.pageId)
	}
	return value, err
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

	return BlockId(left - 1)
}
