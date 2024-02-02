/*
this package is a heap manager that will be used to manage the heap of the database.
--------------------------------------------------------------------
heap header structure
number of pages used in the heap (4 bytes)
--------------------------------------------------------------------


--------------------------------------------------------------------
page structure
page header
--------------------------------------------------------------------

*/

package heapmanager

import (
	"fmt"
	"os"
)

const pageSize = 4096

// function to create a new heap
func CreateHeap(name string) {
	//call os to create a new file with name.data
	_, err := os.Create(name + ".data")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", name, err))
	}
}
