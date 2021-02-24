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
	for i := 0; i < 10000; i++ {
		key := strconv.Itoa(i)
		value, err := cache.Get(key)
		if err != nil {
			log.Println("get error: ", err)
		} else {
			fmt.Println("value: ", value)
		}
	}
}
