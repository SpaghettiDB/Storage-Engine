package main

import (
	"database/src/indexmanager"
	"encoding/binary"
	"fmt"
)

func main() {
	// HeapManager PlayGround ----------------------------------------------------------------

	// heapmanager.PlayGround()

	//IndexManager-------------------------------------------------------------------

	// indexmanager.PlayGround()

	// indexmanager.InitializeIndex("test", "test", "test", false)

	//scan test -------------------------------------------------------------------

	err := indexmanager.InitializeIndex("Student", "name", "name", false)
	if err != nil {
		fmt.Println(err)
	}

	err = indexmanager.InitializeIndex("Student", "id", "id", false)
	if err != nil {
		fmt.Println(err)
	}

	d := make([]byte, 4)
	binary.BigEndian.PutUint32(d, 2)

	indexmanager.AddEntryToTableIndexes("Student", [][]byte{[]byte("mohammed"), d}, 2)

	result, err := indexmanager.GetIndexSize("Student", "name")

	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(result)

}
