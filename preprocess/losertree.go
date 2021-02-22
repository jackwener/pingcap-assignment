package preprocess

import (
	"bufio"
	"fmt"
	"io"
	"kv/constant"
	"log"
	"os"
	"strconv"
)

type LoserTree struct {
	node   []int     // 败者树的中间节点，保存比较结果，由于是完全二叉树，所以可以使用一维数组来表示
	leaves []KVEntry // 叶子节点，保存数据
	k      int
}

func CreateLoserTree(k int) LoserTree {
	lt := LoserTree{
		node:   make([]int, k),
		leaves: make([]KVEntry, k+1),
		k:      k,
	}

	return lt
}

func (lt LoserTree) InitLoserTree() {
	lt.leaves[lt.k].Key = constant.MinKey
	//设置ls数组中败者的初始值
	for i := 0; i < lt.k; i++ {
		lt.node[i] = lt.k
	}
	//对于每一个叶子结点，调整败者树中非终端结点中记录败者的值
	for i := lt.k - 1; i >= 0; i-- {
		lt.Adjust(i)
	}
}

//沿从叶子结点leaves[s]到根结点node[0]的路径调整败者树
func (lt LoserTree) Adjust(s int) {
	t := (s + lt.k) / 2
	for {
		if t <= 0 {
			break
		}
		if lt.leaves[s].Key > lt.leaves[lt.node[t]].Key {
			lt.node[t], s = s, lt.node[t]
		}
		t = t / 2
	}
	//最终将胜者的值赋给 ls[0]
	lt.node[0] = s
}

//败者树的建立及内部归并
func (lt LoserTree) KMerge() {
	//模拟从外存中的5个初始归并段中向内存调取数据
	for i := 0; i <= lt.k; i++ {
		lt.input(i)
	}

	lt.InitLoserTree()

	//最终的胜者存储在 is[0]中，当其值为 MaxKey时，证明5个临时文件归并结束
	for {
		if lt.leaves[lt.node[0]].Key == constant.MaxKey {
			break
		}
		//输出过程模拟向外存写的操作
		fmt.Println(lt.leaves[lt.node[0]].Key)
		//继续读入后续的记录
		lt.input(lt.node[0])
		//根据新读入的记录的关键字的值，重新调整败者树，找出最终的胜者
		lt.Adjust(lt.node[0])
	}
}

func (lt LoserTree) input(id int) {
	file, err := os.Create("intern-" + strconv.Itoa(id) + ".txt")
	if err != nil {
		panic("open file failed")
	}
	defer file.Close()

	// dataBuf := make([]byte, 2^8)
	lenBuf := make([]byte, 4)

	r := bufio.NewReader(file)
	n, err := r.Read(lenBuf)
	if err != nil || n != 4 {
		if err == io.EOF {

		}
		log.Printf("err: %v, read length byte: %v\n", err, n)
	}
}
