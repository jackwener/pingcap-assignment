package buffer

import (
	"bufio"
	"errors"
	"io"
	"kv/preprocess"
	"log"
	"os"
	"strconv"
	"strings"
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
	left := 0
	right := kvs.length - 1
	var mid int
	for {
		if left > right {
			break
		}

		mid = left + (right-left)/2
		if strings.Compare(key, kvs.keys[mid]) > 0 {
			left = mid + 1
		} else if strings.Compare(key, kvs.keys[mid]) < 0 {
			right = mid - 1
		} else {
			return mid, nil
		}
	}

	// TODO
	return -1, errors.New("binarySearch find key error")
}

func (page *Page) loadPage(id BlockId) {
	page.blockId = id

	file, err := os.Open("block" + "-" + strconv.Itoa(int(id)) + ".txt")
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
