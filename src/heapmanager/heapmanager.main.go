
package heapmanager

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

const (
	pageSize       = 8192
	pageHeaderSize = 4
	heapHeaderSize = 8
)

func CreateHeap(name string) error {
	//check if the file already exists

	// if _, err := os.Stat(name); os.IsNotExist(err) {
	// 	return errors.New("file already exists")
	// }

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
	file.Sync()

	//create the first page and write it to the file
	page := createPage()
	appendPageToHeap(file, page)
	return nil
}

// adds a new row to the heap with name.
// TODO : should return the page number where the row was added.
func AddRowToHeap(name string, row []byte) {
	

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
	page, _ := getPageFromHeap(file, int(pageCount-1))
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

		//update the heap header with the new rowCount
		//read the header in byte slice
		header = make([]byte, heapHeaderSize)
		if _, err := file.ReadAt(header, 0); err != nil {
			return
		}

		//parse the header
		_, rowCount := parseHeapHeader(header)

		//update the rowCount
		rowCount++
		binary.BigEndian.PutUint32(header[4:8], rowCount)

		//write the header to the file
		file.WriteAt(header, 0)
		file.Sync()

	} else {

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
	if pageIndex >= int(pageCount) {
		return nil
	}

	page, _ := getPageFromHeap(file, pageIndex)
	rows := extractRowsFromPage(page)

	return rows
}

// returns the row with index = rowIndex from the heap with name = name.
func GetRowFromHeap(name string, rowIndex int) ([]byte, error) {

	file, err := os.OpenFile(name, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// read the header in byte slice
	header := make([]byte, heapHeaderSize)
	if _, err := file.ReadAt(header, 0); err != nil {
		return nil, err
	}

	pageCount, rowCount := parseHeapHeader(header)

	// rowCount := binary.LittleEndian.Uint32(header[:4])
	if rowIndex >= int(rowCount) {
		return nil, errors.New("row index out of range")
	}
	// Initialize variables for tracking page index and remaining rows to find
	remainingRows := rowIndex
	var pageIndex int

	// Iterate through each page until we find the page containing the row
	for pageIndex = 0; pageIndex < int(pageCount); pageIndex++ {
		// Get the page from the heap file
		page, _ := getPageFromHeap(file, pageIndex)
		if page == nil {
			return nil, errors.New("failed to retrieve page")
		}

		// Parse page header to get the number of records and free space offset
		_, recordCount := parsePageHeader(page)

		// Check if the row is in this page
		if remainingRows < int(recordCount) {
			// Calculate the offset of the row within the page
			rowOffset := pageHeaderSize
			for i := 0; i < remainingRows; i++ {
				recordSize := binary.BigEndian.Uint16(page[rowOffset : rowOffset+2])
				rowOffset += int(recordSize) + 2 // 2 bytes for record size
			}
			// Extract and return the row
			recordSize := binary.BigEndian.Uint16(page[rowOffset : rowOffset+2])
			return page[rowOffset+2 : rowOffset+2+int(recordSize)], nil
		} else {
			// Move to the next page
			remainingRows -= int(recordCount)
		}
	}

	// If the loop completes without finding the row, return an error
	return nil, errors.New("row not found")
}


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

	// fmt.Println("extractRowsFromPage ", page)

	_, recordCount := parsePageHeader(page)

	fmt.Println("recordCount ", recordCount)

	records := make([][]byte, 0)

	//skip the header size
	recordIndex := pageHeaderSize

	for recordCount > 0 {
		//read the row size from row header
		rowSize := binary.BigEndian.Uint16(page[recordIndex : recordIndex+2])

		//read the row from the page
		row := make([]byte, rowSize)
		copy(row, page[recordIndex+2:recordIndex+2+int(rowSize)])
		records = append(records, row)

		//update the index to get the next row
		recordIndex = recordIndex + 2 + int(rowSize)
		recordCount -= 1
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
func getPageFromHeap(file *os.File, pageIndex int) ([]byte, error) {

	offset := int64(heapHeaderSize + pageIndex*pageSize)

	// Seek to the beginning of the page
	if _, err := file.Seek(offset, 0); err != nil {
		return nil, err
	}

	// Read the page content
	page := make([]byte, pageSize)
	if _, err := file.Read(page); err != nil {
		return nil, err
	}

	return page, nil
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
	//write the page to the end of the file
	//get file size and write the page to the end of the file

	// Get the current file size
	fileInfo, err := file.Stat()
	if err != nil {
		return
	}
	fileSize := fileInfo.Size()

	// Write the page to the end of the file
	file.WriteAt(page, fileSize)

	//read heap header from the file and parse it then ++ pageCount

	header := make([]byte, heapHeaderSize)
	file.ReadAt(header, 0)
	pageCount, _ := parseHeapHeader(header)
	pageCount++
	binary.BigEndian.PutUint32(header, pageCount)

	//write the heap header to the file
	file.WriteAt(header, 0)
	file.Sync()
}
