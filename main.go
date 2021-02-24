package main

import (
	"fmt"
	"kv/buffer"
	"kv/preprocess"
	"log"
	"strconv"
)

func main() {
	blockFirstKey := preprocess.SortAll()
	cache := buffer.CreateMemCache(blockFirstKey)

	testMulti(cache)

}

func testSingle(cache *buffer.MemCache) {
	for i := 0; i < 100000; i++ {
		key := strconv.Itoa(i)
		_, err := cache.Get(key)
		if err != nil {
			log.Println("get error: ", err)
		} else {
		}
	}
}

func testMulti(cache *buffer.MemCache) {
	for i := 0; i < 1000; i++ {
		go func(i int) {
			var key string
			for j := 0; j < 100; j++ {
				key = strconv.Itoa(i*100 + j)
				fmt.Println(key)
			}

			value, err := cache.Get(key)
			if err != nil {
				log.Println("get error: ", err)
			} else {
				fmt.Println("value: ", value)
			}
		}(i)
	}
}
