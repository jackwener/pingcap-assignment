package preprocess

import (
	"bufio"
	"io"
	"kv/util"
	"log"
	"os"
	"sort"
)

type KVEntry struct {
	Key   string
	Value string
}

type KVEntries []KVEntry

func (e KVEntries) Len() int           { return len(e) }
func (e KVEntries) Less(i, j int) bool { return e[i].Key < e[j].Key }
func (e KVEntries) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

func SortAll() []string {
	checkDir()
	lt := CreateLoserTree(Sort())
	blockFirstKey := lt.KMerge()
	lt.CloseLoserTree()

	return blockFirstKey
}

func checkDir() {
	blockPath := "./block/"
	internPath := "./intern/"

	paths := []string{blockPath, internPath}

	for _, path := range paths {
		// check
		if _, err := os.Stat(path); err != nil {
			err := os.Mkdir(path, 0711)

			if err != nil {
				log.Fatal("Error creating directory")
				return
			}
		}

		// check again
		if _, err := os.Stat(path); err != nil {
			log.Fatal("Error not found directory")
			return
		}
	}
}

func Sort() int {
	internId := 0

	file, err := os.Open("./test.txt")
	if err != nil {
		panic("open file failed")
	}
	defer file.Close()

	// readCount := 0

	r := bufio.NewReader(file)
	dataBytes := make([]byte, 2^8)
	lenBytes := make([]byte, 4)

	kvs := make(KVEntries, 10000)

	for {
		for i, _ := range kvs {
			// read key
			length, err := ReadInt32(r, lenBytes)
			if err != nil {
				if err == io.EOF {
					sort.Sort(kvs)
					OutputInternFile(kvs, internId)

					// 返回的是count，所以+1
					return internId + 1
				}
				util.Check(err)
			}

			key, err := ReadString(r, dataBytes[:length], length)
			kvs[i].Key = key

			// read value
			value, err := ReadStr(r, lenBytes, dataBytes)

			kvs[i].Value = value
		}

		sort.Sort(kvs)
		OutputInternFile(kvs, internId)
		internId++
	}

	// 返回的是count，所以+1
	return internId + 1
}
