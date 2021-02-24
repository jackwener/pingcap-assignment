package buffer

import (
	"bufio"
	"errors"
	"kv/preprocess"
	"os"
	"strings"
)

type Page struct {
	PageId   PageId
	PageType int
	Pin      int
	Store    SortedKVEntries
	// RwLock   sync.RWMutex
}

type SortedKVEntries struct {
	keys   []string
	values []string
	length int
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

	return -1, errors.New("")
}

// func initPage(page)

func (page *Page) loadPage(id PageId) {
	file, err := os.Open("page"+"-" + string(id) +".txt")
	if err != nil {
		
	}
	defer file.Close()
	
	r := bufio.NewReader(file)
	dataBytes := make([]byte, 2^8)
	lenBytes := make([]byte, 4)
	
	for i := 0; i < 1000; i++ {
		key, err := preprocess.ReadStr(r,lenBytes,dataBytes)
		if err != nil {
			
		}
		value, err := preprocess.ReadStr(r,lenBytes,dataBytes)
		if err != nil {
			
		}
		page.Store.keys[i] = key
		page.Store.values[i] = value
	}
}
