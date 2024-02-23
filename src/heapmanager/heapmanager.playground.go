package heapmanager

import (
	"fmt"
)

func PlayGround() {
	CreateHeap("student")
	AddRowToHeap("student", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	rows := GetPageRowsFromHeap("student", 0)
	fmt.Println(rows)
}
