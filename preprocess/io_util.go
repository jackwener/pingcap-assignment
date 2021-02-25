package preprocess

import (
	"bufio"
	"kv/util"
	"log"
	"os"
	"strconv"
)

func OutputInternFile(kvs KVEntries, id int) {
	file, err := os.Create("./intern/intern" + "-" + strconv.Itoa(id) + ".txt")
	if err != nil {
		panic("open file failed")
	}
	defer file.Close()

	w := bufio.NewWriter(file)

	lenBytes := make([]byte, 0, 4)

	for _, kv := range kvs {
		err := WriteLength(w, lenBytes, int32(len(kv.Key)))
		util.Check(err)

		err = WriteString(w, kv.Key)
		util.Check(err)

		err = WriteLength(w, lenBytes, int32(len(kv.Value)))
		util.Check(err)

		err = WriteString(w, kv.Value)
		util.Check(err)
	}

	w.Flush()
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

func WriteStr(w *bufio.Writer, bytes_ []byte, str string) {
	err := WriteLength(w, bytes_, int32(len(str)))
	util.Check(err)

	err = WriteString(w, str)
	util.Check(err)
}

func WriteLength(w *bufio.Writer, bytes_ []byte, length int32) error {

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

func WriteString(w *bufio.Writer, str string) error {
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
