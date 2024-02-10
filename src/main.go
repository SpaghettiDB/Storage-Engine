package main

import (
	"database/src/heapmanager"
)

func main() {

	heapmanager.CreateHeap("test")
	heapmanager.AddRowToHeap("test", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	heapmanager.GetRowFromHeap("test", 0)

}
