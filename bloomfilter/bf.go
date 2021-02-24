package bloomfilter

import (
	"kv/util"
	"math"
	"sync"

	"github.com/spaolacci/murmur3"
)

type Filter struct {
	lock       *sync.RWMutex
	concurrent bool

	m     uint64 // m: 过滤器的大小
	n     uint64 // n: 插入的元素数
	log2m uint64
	k     uint64 // k: hash函数的个数
	keys  []byte
}

func New(size uint64, k uint64, race bool) *Filter {
	log2 := uint64(math.Ceil(math.Log2(float64(size))))
	filter := &Filter{
		m:          1 << log2,
		log2m:      log2,
		k:          k,
		keys:       make([]byte, 1<<log2),
		concurrent: race,
	}
	if filter.concurrent {
		filter.lock = &sync.RWMutex{}
	}
	return filter
}

func (f *Filter) Add(data []byte) *Filter {
	if f.concurrent {
		f.lock.Lock()
		defer f.lock.Unlock()
	}
	h := baseHash(data)
	for i := uint64(0); i < f.k; i++ {
		loc := location(h, i)
		slot, mod := f.location(loc)
		f.keys[slot] |= 1 << mod
	}
	f.n++
	return f
}

// Test 检查 bloom过滤器 中是否可能存在 byte array
func (f *Filter) Test(data []byte) bool {
	if f.concurrent {
		f.lock.RLock()
		defer f.lock.RUnlock()
	}
	h := baseHash(data)
	for i := uint64(0); i < f.k; i++ {
		loc := location(h, i)
		slot, mod := f.location(loc)
		if f.keys[slot]&(1<<mod) == 0 {
			return false
		}
	}
	return true
}

func (f *Filter) AddString(s string) *Filter {
	data := util.StrToBytes(s)
	return f.Add(data)
}

func (f *Filter) TestString(s string) bool {
	data := util.StrToBytes(s)
	return f.Test(data)
}

func (f *Filter) location(h uint64) (uint64, uint64) {
	slot := (h / BitPerByte) & (f.m - 1)
	mod := h & Mod7
	return slot, mod
}

func location(h []uint64, i uint64) uint64 {
	// return h[ii%2] + ii*h[2+(((ii+(ii%2))%4)/2)]
	return h[i&1] + i*h[2+(((i+(i&1))&3)/2)]
}

// baseHash returns the murmur3 128-bit hash
// 为什么要用murmur3的hash算法 ：https://softwareengineering.stackexchange.com/questions/49550/which-hashing-algorithm-is-best-for-uniqueness-and-speed
func baseHash(data []byte) []uint64 {
	a1 := []byte{1}
	hasher := murmur3.New128()
	hasher.Write(data)
	v1, v2 := hasher.Sum128()
	hasher.Write(a1)
	v3, v4 := hasher.Sum128()
	return []uint64{
		v1, v2, v3, v4,
	}
}
