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

	var b []byte = make([]byte, 4)
	binary.BigEndian.PutUint32(b, 1)
	indexmanager.AddEntryToTableIndexes("test", b, 1)

	binary.BigEndian.PutUint32(b, 2)
	indexmanager.AddEntryToTableIndexes("test", b, 2)

	binary.BigEndian.PutUint32(b, 3)
	indexmanager.AddEntryToTableIndexes("test", b, 3)

	binary.BigEndian.PutUint32(b, 1)
	var c []byte = make([]byte, 4)
	binary.BigEndian.PutUint32(c, 3)

	d := make([]byte, 4)
	binary.BigEndian.PutUint32(d, 2)

	result, err := indexmanager.ScanIndexRange("test", "test", b, c)

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(result)
	}

	indexmanager.RemoveEntryFromTableIndexes("test", d)

	result, err = indexmanager.ScanIndexRange("test", "test", b, c)

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(result)
	}

}
