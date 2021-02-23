package preprocess

import (
	"bufio"
	"io"
	"kv/util"
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

func SortAll(internCount int) {
	lt := CreateLoserTree(internCount)
	lt.KMerge()
	lt.CloseLoserTree()
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
					return internId
				}
				util.Check(err)
			}

			key, err := ReadString(r, dataBytes[:length], length)
			kvs[i].Key = key

			// read value
			length, err = ReadInt32(r, lenBytes)
			util.Check(err)

			value, err := ReadString(r, dataBytes[:length], length)
			kvs[i].Value = value
		}

		sort.Sort(kvs)
		OutputInternFile(kvs, internId)
		internId++
	}

	return internId
}

func ReadStr(r *bufio.Reader, lenBytes []byte, dataBytes []byte) (string, error) {
	length, err := ReadInt32(r, lenBytes)
	if err != nil {
		return "", err
	}

	key, err := ReadString(r, dataBytes[:length], length)
	if err != nil {
		return "", err
	}
	return key, nil
}
