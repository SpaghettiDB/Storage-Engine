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
	"database/src/errors"
	"encoding/binary"
	"os"
)

const (
	pageSize       = 8192
	pageHeaderSize = 4
	heapHeaderSize = 8
)

// creates a new heap file with file name = name and initializes the heap header.
// after calling this function it's expected to find a file with name = name
// and the size of the file should be equal to the header size.

func CreateHeap(name string) error {
	//check if the file already exists
	if _, err := os.Stat(name); os.IsNotExist(err) {
		return &errors.ResourceAlreadyExistsError{
			ResourceType: errors.Heap,
			ResourceName: name,
		}
	}

	file, err := os.Create(name)
	if err != nil {
		return err
	}

	// set the file permission to 0644
	if err := file.Chmod(0644); err != nil {
		return err
	}

	defer file.Close()

	header := make([]byte, heapHeaderSize)
	if _, err := file.WriteAt(header, 0); err != nil {
		return err
	}
	return nil
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

	file, err := os.OpenFile(name, os.O_RDWR, 0644)
	if err != nil {
		return
	}

	//read the header in byte slice
	header := make([]byte, heapHeaderSize)
	if _, err := file.ReadAt(header, 0); err != nil {
		return
	}

	pageCount, _ := parseHeapHeader(header)
	page := getPageFromHeap(file, int(pageCount-1))
	freeSpaceOffset, recordCount := parsePageHeader(page)

	//calculate the free space available in the page
	freeSpaceAvailable := pageSize - freeSpaceOffset

	//if the free space available is enough to add the row then go ahead
	//notice that we add 2 to the length of the row to store the record size

	if int(freeSpaceAvailable) >= len(row)+2 {
		//write the row with its size to the page
		//update the page header

		//get the record size and convert it to byte slice
		recordSize := make([]byte, 2)
		binary.BigEndian.PutUint16(recordSize, uint16(len(row)))

		//write the record size to the page
		copy(page[freeSpaceOffset:], recordSize)

		//write the row to the page
		copy(page[freeSpaceOffset+2:], row)

		//update the page header
		freeSpaceOffset += uint16(len(row) + 2)
		recordCount++
		binary.BigEndian.PutUint16(page[0:2], freeSpaceOffset)
		binary.BigEndian.PutUint16(page[2:4], recordCount)

		//overWrite the page to the file
		overWritePageToHeap(file, int(pageCount-1), page)
	} else {

		//if the free space available is not enough to add the row then add new page
		//initialize the new page and write the row to it
		//update the heap header
		//create new page
		newPage := createPage()
		//append the new page to the file
		appendPageToHeap(file, newPage)

		//call the function recursively to add the row to the new page
		AddRowToHeap(name, row)
	}

}

// returns all the rows from the heap with name = name and page index = pageIndex.
func GetPageRowsFromHeap(name string, pageIndex int) [][]byte {
	file, err := os.OpenFile(name, os.O_RDWR, 0644)

	if err != nil {
		return nil
	}

	header := make([]byte, heapHeaderSize)
	if _, err := file.ReadAt(header, 0); err != nil {
		return nil
	}

	pageCount, _ := parseHeapHeader(header)
	if pageIndex > int(pageCount) {
		return nil
	}

	page := getPageFromHeap(file, pageIndex)
	rows := extractRowsFromPage(page)

	return rows
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
	if len(page) != pageSize {
		return 0, 0
	}

	freeSpaceOffset := binary.BigEndian.Uint16(page[0:2])
	recordCount := binary.BigEndian.Uint16(page[2:4])

	return freeSpaceOffset, recordCount
}

// parse the heap header and return the pageCount and rowCount
func parseHeapHeader(header []byte) (uint32, uint32) {

	if len(header) != heapHeaderSize {
		return 0, 0
	}
	pageCount := uint32(header[0])<<24 | uint32(header[1])<<16 | uint32(header[2])<<8 | uint32(header[3])
	rowCount := uint32(header[4])<<24 | uint32(header[5])<<16 | uint32(header[6])<<8 | uint32(header[7])

	return pageCount, rowCount
}

// takes a page and returns all the rows in the page
func extractRowsFromPage(page []byte) [][]byte {
	_, recordCount := parsePageHeader(page)
	records := make([][]byte, 0)

	//skip the header size
	recordIndex := pageHeaderSize

	for recordCount > 0 {
		//read the row size from row header
		rowSize := binary.BigEndian.Uint16(page[recordIndex : recordIndex + 2])

		//read the row from the page
		row := make([]byte, rowSize)
		copy(row, page[recordIndex + 2 : recordIndex + 2 + int(rowSize)])
		records = append(records, row)

		//update the index to get the next row
		recordIndex = recordIndex + 2 + int(rowSize)
		recordCount -= recordCount
	}
	return records
}

// crete new page and initialize page header with free space offset = 0 and record count = 0
// return the page as []byte
func createPage() []byte {
	page := make([]byte, pageSize)

	copy(page[0:2], []byte{byte(pageHeaderSize >> 8), byte(pageHeaderSize)})
	copy(page[2:4], []byte{0, 0})
	return page
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
	file.WriteAt(page, int64(pageIndex*pageSize)+int64(heapHeaderSize))
	file.Sync()
}

// append the page to the file
func appendPageToHeap(file *os.File, page []byte) {
	file.Write(page)
	file.Sync()
}
