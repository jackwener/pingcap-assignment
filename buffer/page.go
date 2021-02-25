package buffer

import (
	"bufio"
	"errors"
	"io"
	"kv/preprocess"
	"log"
	"os"
	"strconv"
	"sync"
)

type Page struct {
	pageId  PageId
	blockId BlockId
	pin     int32
	store   SortedKVEntries

	rwLock *sync.RWMutex
}

type SortedKVEntries struct {
	keys   []string
	values []string
	length int
}

func (page *Page) initPage(id PageId) {
	var rwLock sync.RWMutex

	page.rwLock = &rwLock
	page.pageId = id
	page.blockId = -1
	page.pin = 1

	page.store.keys = make([]string, PageSlotNum)
	page.store.values = make([]string, PageSlotNum)
	page.store.length = 0
}

//func (page *Page) clear() {
//
//}

func (page *Page) get(key string) (string, error) {
	//page.rwLock.RLock()
	//defer page.rwLock.RUnlock()
	index, err := page.store.binarySearch(key)
	if err != nil {
		return "", err
	}
	return page.store.values[index], nil
}

func (kvs *SortedKVEntries) binarySearch(key string) (int, error) {
	head := 0
	tail := kvs.length - 1

	for head <= tail {
		mid := (head + tail) / 2
		if kvs.keys[mid] == key {
			return mid, nil
		}

		if kvs.keys[mid] < key {
			head = mid + 1
		} else {
			tail = mid - 1
		}
	}

	// TODO
	return -1, errors.New("binarySearch find key error")
}

func (page *Page) loadPage(id BlockId) {
	//page.rwLock.Lock()
	//defer page.rwLock.Unlock()

	page.pin = 1
	page.blockId = id

	file, err := os.Open("./block/block" + "-" + strconv.Itoa(int(id)) + ".txt")
	if err != nil {
		log.Fatalln("open block error")
	}
	defer file.Close()

	r := bufio.NewReader(file)
	dataBytes := make([]byte, 2^8)
	lenBytes := make([]byte, 4)

	for i := 0; i < preprocess.BlockSize; i++ {
		key, err := preprocess.ReadStr(r, lenBytes, dataBytes)
		if err != nil {
			if err == io.EOF {
				log.Fatalln("page size not eq block size, error")
			}
		}
		value, err := preprocess.ReadStr(r, lenBytes, dataBytes)
		if err != nil {

		}
		page.store.keys[i] = key
		page.store.values[i] = value
		page.store.length = i + 1
	}
}
