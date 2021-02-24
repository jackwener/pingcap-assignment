package buffer

import (
	"errors"
	"fmt"
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
	blockId := c.GetBlockId(key)

	if key == "20" {
		fmt.Println(249)
	}

	if blockId == -1 {
		return "", errors.New("not found block")
	}
	page := c.bufferPoolManager.fetchPage(blockId)
	return page.store.get(key)
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
