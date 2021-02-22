package util

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

//整形转换成字节
func IntToBytes(n int32) []byte {

	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, n)
	return bytesBuffer.Bytes()
}

//字节转换成整形
func BytesToInt(b []byte) int32 {
	bytesBuffer := bytes.NewBuffer(b)

	var n int32
	binary.Read(bytesBuffer, binary.BigEndian, &n)

	return n
}

func StrToBytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	b := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&b))
}

func BytesToStr(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func Check(e error) {
	if e != nil {
		panic(e)
	}
}
