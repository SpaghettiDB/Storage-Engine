package main

import (
	"database/src/heapmanager"
	"database/src/indexmanager"
	"fmt"
)

func main() {

	heapmanager.CreateHeap("student")
	heapmanager.AddRowToHeap("student", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	heapmanager.AddRowToHeap("student", []byte{11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	heapmanager.AddRowToHeap("student", []byte{11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	heapmanager.AddRowToHeap("student", []byte{11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	heapmanager.AddRowToHeap("student", []byte{11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	heapmanager.AddRowToHeap("student", []byte{11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	heapmanager.AddRowToHeap("student", []byte{11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	rows := heapmanager.GetPageRowsFromHeap("student", 0)
	fmt.Println(rows)

	//------------------------------------------------------------------------------------------

	indexmanager.PlayGround()

}
