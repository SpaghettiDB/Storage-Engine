/*
this package is a heap manager that will be used to manage the heap of the database.
please read the heapfile format in docs carefully we all should be consistent with the format.

this module should only work with binary data and should not be aware of the data types of the columns.
it just take []byte and write it to the file.
it just read the []byte from the file and return it to the caller.
parsing the data types and the columns should be done in the higher level modules.


while we write the public functions we may need to write some helper functions that will be used internally.
please announce the need of specific helper function on slack so we can discuss and reuse logic .

writing unit tests is really appreciated.
*/

package heapmanager

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
	//update the page header
}

// returns all the rows from the heap with name = name and page index = pageIndex.
func GetPageFromHeap(name string, pageIndex int) [][]byte {
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
