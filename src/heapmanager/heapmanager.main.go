/*

PACKAGE NAME : heapmanager
this package is a heap manager that will be used to manage the heap of the database.
please read the heapfile format in docs carefully we all should be consistent with the format.

RESPONSIBILITIES :
this module should only work with binary data and should not be aware of the data types of the columns.
it just take []byte and write it to the file.
it just read the []byte from the file and return it to the caller.
parsing the data types and the columns should be done in the higher level modules.

CODE REUSE :
while writing the public functions we may need to write some helper functions
please announce the need of specific helper function on slack so we can discuss and reuse logic .

TESTING :
writing unit tests is really appreciated :)

TODO :
- For now we won't use slot array we will just traverse records by reading record size
- For now we won't use free-list, our heap will grow indefinitely
- use binary search for getting row with specific index

MOTIVATION :
- move fast and break things :D

i know skipping some staff like slot array or free-list is not a good idea
but i always prefer to have something that works and then improve it
trust the process and let's move fast and break things

*/

package heapmanager

import (
	"os"
)

const pageSize = 8192
const pageHeaderSize = 4
const heapHeaderSize = 8

// creates a new heap file with file name = name and initializes the heap header.
// after calling this function it's expected to find a file with name = name
// and the size of the file should be equal to the header size.
func CreateHeap(name string) {
	//you can use os package to create a file
	//truncate function can be used to set the size of the file
	//use the file.WriteAt function to write the header to the file
}

// adds a new row to the heap with name.
func AddRowToHeap(name string, row []byte) {
	//open the file
	//read the header
	//read pageCount from the header and go read this page
	//then read the free space available in the page if it's enough go ahead
	//if not then it's time to add new page to this heap then add the row
	//you can use Sync function to flush to ensure durability
	//update the page header
}

// returns all the rows from the heap with name = name and page index = pageIndex.
func GetPageRowsFromHeap(name string, pageIndex int) [][]byte {
	//open the file
	//read the header
	//read the pageCount from the header
	//if the pageIndex is greater than the pageCount then return nil directly else
	//read the page from the file and return its record as [][]byte
	return nil
}

// returns the row with index = rowIndex from the heap with name = name.
func GetRowFromHeap(name string, rowIndex int) []byte {
	//open the file
	//read the header
	//read the rowCount from the header
	//if the rowIndex is greater than the rowCount then return nil directly else
	//get the first page rowCount from its header
	//if the rowIndex is less than the rowCount then go ahead and read the row from this page
	//if not then subtract the rowCount from the rowIndex and go to the next page and so on
	//when you find the page that contains the row then read it and return the required row
	//please take care of difference between rowIndex and rowCount in page header
	//this function can reuse logic of GetPageFromHeap,
	//you just need to deduce the page index as the previous explanation
	return nil
}

//-------------private helper functions -------------------

// takes a page and returns freeSpaceOffset and recordCount
func parsePageHeader(page []byte) (uint16, uint16) {
	return 0, 0
}

// parse the heap header and return the pageCount and rowCount
func parseHeapHeader(header []byte) (uint32, uint32) {
	return 0, 0
}

// takes a page and returns all the rows in the page
func extractRowsFromPage(page []byte) [][]byte {
	return nil
}

// crete new page and initialize page header with free space offset = 0 and record count = 0
// return the page as []byte
func createPage() []byte {
	return nil
}

// returns the page with pageIndex from the heap file
func getPageFromHeap(file *os.File, pageIndex int) []byte {
	//read the page from the file
	//return the page
	//checking out of bound is the responsibility of the caller
	//take care of header size
	return nil
}

// overWrite the page to the file at pageIndex
func overWritePageToHeap(file *os.File, pageIndex int, page []byte) {
	//overWrite the page to the file
	//use the file.WriteAt function
}

// append the page to the file
func appendPageToHeap(file *os.File, page []byte) {
	//use the file.Write function to append the page to the file
}
