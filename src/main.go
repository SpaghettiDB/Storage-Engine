package main

import (
	"database/src/indexmanager"
)

func main() {
	// HeapManager PlayGround ----------------------------------------------------------------

	// heapmanager.PlayGround()

	//IndexManager-------------------------------------------------------------------

	// indexmanager.PlayGround()

	// indexmanager.InitializeIndex("test", "test", "test", false)
	err := indexmanager.AddEntryToTableIndexes("test", []byte("key"), 5)
	if err != nil {
		println(err.Error())
	}

}
