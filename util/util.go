package util

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

func BytesToUint32(b []byte) uint32 {
	var b0, b1, b2, b3 uint32
	var m0, m1, m2, m3 uint32
	m0, m1, m2, m3 = 0xFF000000, 0x00FF0000, 0x0000FF00, 0x000000FF

	b0 = (uint32(b[0]) << 0x18) & m0
	b1 = (uint32(b[1]) << 0x10) & m1
	b2 = (uint32(b[2]) << 0x08) & m2
	b3 = (uint32(b[3]) << 0x00) & m3

	return b0 | b1 | b2 | b3
}

// 确保int32不是负数，因为m0为0x7F000000,最高位为0
// 现在的场景是字节数必>=0, 为啥不用uint？懒得改了
func BytesToInt32(b []byte) int32 {
	var b0, b1, b2, b3 int32
	var m0, m1, m2, m3 int32
	m0, m1, m2, m3 = 0x7F000000, 0x00FF0000, 0x0000FF00, 0x000000FF

	b0 = (int32(b[0]) << 0x18) & m0
	b1 = (int32(b[1]) << 0x10) & m1
	b2 = (int32(b[2]) << 0x08) & m2
	b3 = (int32(b[3]) << 0x00) & m3

	return b0 | b1 | b2 | b3
}

// 注意: bytes_容量应该>=4, len=0
func Int32ToBytes(n int32, bytes_ []byte) []byte {
	bytesBuffer := bytes.NewBuffer(bytes_)
	binary.Write(bytesBuffer, binary.BigEndian, n)
	return bytesBuffer.Bytes()
}

func StrToBytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	b := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&b))
}

func BytesToStr(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func Uint16ToBytes(num uint16) []byte {
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, num)
	return data
}

func Uint32ToBytes(num uint32) []byte {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, num)
	return data
}

func Uint64ToBytes(num uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, num)
	return data
}

/*
func Int32ToBytes(n int32, bytes_ []byte) []byte {

	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, n)
	return bytesBuffer.Bytes()
}

func BytesToInt32(b []byte) int32 {
	bytesBuffer := bytes.NewBuffer(b)

	var n int32
	binary.Read(bytesBuffer, binary.BigEndian, &n)
}
*/

// TODO：需要重改一下整个项目的异常处理
func Check(e error) {
	if e != nil {
		panic(e)
	}
}
