package main

import "kv/preprocess"

func main() {
	count := preprocess.Sort() + 1
	preprocess.SortAll(count)
}
