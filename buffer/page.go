package buffer

import (
	"bufio"
	"errors"
	"io"
	"kv/preprocess"
	"log"
	"os"
	"strconv"
)

type Page struct {
	pageId  PageId
	blockId BlockId
	pin     int32
	store   SortedKVEntries
	// RwLock   sync.RWMutex
	// PageType int
}

type SortedKVEntries struct {
	keys   []string
	values []string
	length int
}

func (page *Page) initPage(id PageId) {
	page.pageId = id
	page.blockId = -1
	page.pin = 1

	page.store.keys = make([]string, PageSlotNum)
	page.store.values = make([]string, PageSlotNum)
	page.store.length = 0
}

// TODO:有必要吗
func (page *Page) clear() {

}

func (kvs *SortedKVEntries) get(key string) (string, error) {
	index, err := kvs.binarySearch(key)
	if err != nil {
		return "", err
	}
	return kvs.values[index], nil
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
	page.pin = 1
	page.blockId = id

	file, err := os.Open("./block/block" + "-" + strconv.Itoa(int(id)) + ".txt")
	if err != nil {
		log.Println("open block error")
	}
	defer file.Close()

	r := bufio.NewReader(file)
	dataBytes := make([]byte, 2^8)
	lenBytes := make([]byte, 4)

	for i := 0; i < preprocess.BlockSize; i++ {
		key, err := preprocess.ReadStr(r, lenBytes, dataBytes)
		if err != nil {
			if err == io.EOF {

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
