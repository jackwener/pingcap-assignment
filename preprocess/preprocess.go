package preprocess

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"kv/util"
	"log"
	"os"
	"sort"
	"strconv"
)

type KVEntry struct {
	Key   string
	Value string
}

type KVEntries []KVEntry

func (e KVEntries) Len() int           { return len(e) }
func (e KVEntries) Less(i, j int) bool { return e[i].Key < e[j].Key }
func (e KVEntries) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

//func dumpKV(id int) error {
//	filename := "" + strconv.Itoa(id)
//
//	filePath := filename
//	f, err := os.Open(filePath)
//	defer f.Close()
//	if err != nil {
//		return err
//	}
//	buf := bufio.NewReader(f)
//
//}

func SortAll(internCount int) {
	lt := CreateLoserTree(internCount)
	lt.KMerge()
}

func Sort() {
	internId := 0

	file, err := os.Open("./test.txt")
	if err != nil {
		panic("open file failed")
	}
	defer file.Close()

	// readCount := 0

	r := bufio.NewReader(file)
	dataBuf := make([]byte, 2^8)
	lenBuf := make([]byte, 4)

	kvs := make(KVEntries, 10000)

	for {
		for i, _ := range kvs {
			// read key
			length, err := ReadInt32(r, lenBuf)
			if err != nil {
				if err == io.EOF {
					return
				}
				util.Check(err)
			}

			key, err := ReadString(r, dataBuf[:length], length)
			kvs[i].Key = key

			fmt.Println("key ", kvs[i].Key)

			// read value
			length, err = ReadInt32(r, lenBuf)
			util.Check(err)

			value, err := ReadString(r, dataBuf[:length], length)
			kvs[i].Value = value

			fmt.Println("key ", kvs[i].Value)

			// TODO: debug
			if kvs[i].Value == "600" {
				fmt.Println("debug")
			}

			fmt.Println("value ", kvs[i].Value)
		}

		sort.Sort(kvs)
		OutputIntern(kvs, internId)
		internId++
	}

	return
}

func ReadInt32(r *bufio.Reader, buf []byte) (int32, error) {
	n, err := r.Read(buf)
	if err != nil {
		return int32(-1), err
	}
	if n != 4 {
		count := n
		for {
			n, err := r.Read(buf[count:4])
			if err != nil {
				return int32(-1), err
			}
			count += n

			if count == 4 {
				break
			}
		}
	}

	length := util.BytesToInt(buf)

	return length, nil
}

func ReadString(r *bufio.Reader, buf []byte, length int32) (string, error) {
	n, err := r.Read(buf)
	if err != nil {
		return "", err
	}
	if n != int(length) {
		count := n
		for {
			n, err := r.Read(buf[count:length])
			if err != nil {
				return "", err
			}
			count += n

			if count == int(length) {
				break
			}
		}
	}

	return string(buf), nil
}

func OutputIntern(sortKVEntries KVEntries, id int) {
	file, err := os.Create("intern-" + strconv.Itoa(id) + ".txt")
	if err != nil {
		panic("open file failed")
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	lenBytes := make([]byte, 4)
	lenBuffer := bytes.NewBuffer(lenBytes)

	for _, sortKVEntry := range sortKVEntries {
		binary.Write(lenBuffer, binary.BigEndian, len(sortKVEntry.Key))
		n, err := w.Write(lenBytes)
		if err != nil || n != 4 {
			log.Printf("write key len err: %v, n: %d \n", err, n)
		}

		n, err = w.Write(util.StrToBytes(sortKVEntry.Key))
		if err != nil || n != len(sortKVEntry.Key) {
			log.Printf("write key len err: %v, n: %d \n", err, n)
		}

		binary.Write(lenBuffer, binary.BigEndian, len(sortKVEntry.Value))
		n, err = w.Write(lenBytes)
		if err != nil || n != 4 {
			log.Printf("write key len err: %v, n: %d \n", err, n)
		}

		n, err = w.Write(util.StrToBytes(sortKVEntry.Value))
		if err != nil || n != len(sortKVEntry.Value) {
			log.Printf("write key len err: %v, n: %d \n", err, n)
		}
	}

	w.Flush()
}
