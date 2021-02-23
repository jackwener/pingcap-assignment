package preprocess

import (
	"bufio"
	"kv/util"
	"log"
	"os"
	"strconv"
)

func OutputInternFile(kvs KVEntries, id int) {
	file, err := os.Create("intern" + "-" + strconv.Itoa(id) + ".txt")
	if err != nil {
		panic("open file failed")
	}
	defer file.Close()

	w := bufio.NewWriter(file)

	lenBytes := make([]byte, 0, 4)

	for _, kv := range kvs {
		err := WriteLen(w, lenBytes, int32(len(kv.Key)))
		util.Check(err)

		err = WriteStr(w, kv.Key)
		util.Check(err)

		err = WriteLen(w, lenBytes, int32(len(kv.Value)))
		util.Check(err)

		err = WriteStr(w, kv.Value)
		util.Check(err)
	}

	w.Flush()
}

func ReadInt32(r *bufio.Reader, bytes_ []byte) (int32, error) {
	n, err := r.Read(bytes_)
	if err != nil {
		return int32(-1), err
	}
	if n != 4 {
		count := n
		for {
			n, err := r.Read(bytes_[count:4])
			if err != nil {
				return int32(-1), err
			}
			count += n

			if count == 4 {
				break
			}
		}
	}

	length := util.BytesToInt32(bytes_)

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

func WriteLen(w *bufio.Writer, bytes_ []byte, length int32) error {

	n, err := w.Write(util.Int32ToBytes(length, bytes_))
	util.Check(err)

	if n != 4 {
		log.Println("write len remain")
		count := n
		for {
			n, err := w.Write(bytes_[count:])
			util.Check(err)
			count += n

			if count != 4 {
				break
			}
		}
	}

	return nil
}

func WriteStr(w *bufio.Writer, str string) error {
	bytes_ := util.StrToBytes(str)
	n, err := w.Write(util.StrToBytes(str))
	util.Check(err)

	if n != len(str) {
		log.Println("write str remain")
		count := n
		for {
			n, err := w.Write(bytes_[count:])
			util.Check(err)

			count += n

			if count != len(str) {
				break
			}
		}
	}

	return nil
}

func (lt *LoserTree) outputPage(kv KVEntry) {
	var err error
	lenBytes := make([]byte, 0, 4)

	if lt.outCount >= 1000 {
		lt.changeOutput()
	}

	err = WriteLen(lt.writer, lenBytes, int32(len(kv.Key)))
	util.Check(err)

	err = WriteStr(lt.writer, kv.Key)
	util.Check(err)

	err = WriteLen(lt.writer, lenBytes, int32(len(kv.Value)))
	util.Check(err)

	err = WriteStr(lt.writer, kv.Value)
	util.Check(err)

	lt.outCount++
}
