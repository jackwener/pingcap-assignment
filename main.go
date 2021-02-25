package main

import (
	"kv/buffer"
	"kv/preprocess"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

func main() {
	blockFirstKey := preprocess.SortAll()
	cache := buffer.CreateMemCache(blockFirstKey)

	testMulti(cache)
}

func random(min, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max-min) + min
}

func testSingle(cache *buffer.MemCache) {
	for i := 0; i < 100000; i++ {
		key := strconv.Itoa(i)
		_, err := cache.Get(key)
		if err != nil {
			log.Println("get error: ", err)
		} else {
			//fmt.Printf("value: %v \n", value)
		}
	}
}

func testMulti(cache *buffer.MemCache) {
	threadNum := 100

	var wg sync.WaitGroup
	wg.Add(threadNum)

	for i := 0; i < threadNum; i++ {
		go func(i int) {
			var key string
			for j := 0; j < 10000; j++ {
				max := (j+1)*100 - 1
				min := j*100 - 1
				key = strconv.Itoa(random(min, max))
				_, err := cache.Get(key)
				if err != nil {
					log.Printf("key: %v , get error: %v \n", key, err)
				}
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
}
