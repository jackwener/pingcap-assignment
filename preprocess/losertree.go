package preprocess

import (
	"bufio"
	"fmt"
	"io"
	"kv/constant"
	"kv/util"
	"os"
	"strconv"
)

type LoserTree struct {
	node   []int     // 败者树的中间节点，保存比较结果，由于是完全二叉树，所以可以使用一维数组来表示
	leaves []KVEntry // 叶子节点，保存数据
	k      int

	files   []*os.File
	readers []*bufio.Reader

	outId    int
	outCount int
	outFile  *os.File
	writer   *bufio.Writer
}

func CreateLoserTree(k int) LoserTree {
	lt := LoserTree{
		node:   make([]int, k),
		leaves: make([]KVEntry, k),
		k:      k,
	}

	var err error
	lt.files = make([]*os.File, lt.k)
	for i := 0; i < lt.k; i++ {
		lt.files[i], err = os.Open("intern" + "-" + strconv.Itoa(i) + ".txt")
		util.Check(err)
	}

	lt.readers = make([]*bufio.Reader, lt.k)
	for i := 0; i < lt.k; i++ {
		lt.readers[i] = bufio.NewReaderSize(lt.files[i], 1024*1024)
	}

	lt.outCount = 0
	lt.outId = 0
	lt.outFile, err = os.Create("page" + "-" + strconv.Itoa(lt.outId) + ".txt")
	if err != nil {

	}
	lt.writer = bufio.NewWriter(lt.outFile)

	return lt
}

func (lt *LoserTree) InitLoserTree() {
	winner := 0
	for i := 0; i < lt.k; i++ {
		if lt.beat(i, winner) {
			winner = i
		}
	}

	for i := 0; i < lt.k; i++ {
		lt.node[i] = winner
	}

	for i := lt.k - 1; i >= 0; i-- {
		lt.Adjust(i)
	}
}

func (lt *LoserTree) CloseLoserTree() {
	for i := 0; i < lt.k; i++ {
		lt.files[i].Close()
	}
	lt.outFile.Close()
}

//沿从叶子结点leaves[s]到根结点node[0]的路径调整败者树
func (lt *LoserTree) Adjust(s int) {
	t := (s + lt.k) / 2
	for {
		if t <= 0 {
			break
		}
		if lt.beat(lt.node[t], s) {
			lt.node[t], s = s, lt.node[t]
		}
		t = t / 2
	}
	//最终将胜者的值赋给 ls[0]
	lt.node[0] = s
}

//败者树的建立及内部归并
func (lt *LoserTree) KMerge() {
	//模拟从外存中的5个初始归并段中向内存调取数据
	for i := 0; i < lt.k; i++ {
		lt.input(i)
	}

	lt.InitLoserTree()

	//最终的胜者存储在 is[0]中，当其值为 MaxKey时，证明5个临时文件归并结束
	for {
		if lt.leaves[lt.node[0]].Key == constant.MaxKey {
			break
		}
		//向外存写的操作
		lt.outputPage(lt.leaves[lt.node[0]])
		fmt.Println(lt.leaves[lt.node[0]].Key)
		//继续读入后续的记录
		lt.input(lt.node[0])
		//根据新读入的记录的关键字的值，重新调整败者树，找出最终的胜者
		lt.Adjust(lt.node[0])
	}
}

func (lt *LoserTree) input(id int) {
	dataBytes := make([]byte, 2^8)
	lenBytes := make([]byte, 4)

	key, err := ReadStr(lt.readers[id], lenBytes, dataBytes)
	if err != nil {
		if err == io.EOF {
			lt.leaves[id].Key = constant.MaxKey
			return
		}
	}

	value, err := ReadStr(lt.readers[id], lenBytes, dataBytes)
	if err != nil {

	}

	lt.leaves[id].Key = key
	lt.leaves[id].Value = value
}

// 小于
func (lt *LoserTree) beat(index1 int, index2 int) bool {
	t1 := lt.leaves[index1].Key
	t2 := lt.leaves[index2].Key
	if t1 == constant.MaxKey {
		return false
	}
	if t2 == constant.MaxKey {
		return true
	}

	return !(t1 > t2)
}

func (lt *LoserTree) changeOutput() {
	lt.outId++
	lt.outCount = 0

	file, err := os.Create("page" + "-" + strconv.Itoa(lt.outId) + ".txt")
	if err != nil {

	}
	// 注意close前一个File，否则内存泄漏
	lt.outFile.Close()
	lt.writer = bufio.NewWriter(file)
}
