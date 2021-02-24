package buffer


type replacer interface {
	Insert(page *Page) bool
	Victim(page *Page) bool
	Erase(page *Page) bool
}