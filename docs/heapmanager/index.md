# HeapManager

The `HeapManager` is the package that manages the heap of each table in the database. It is responsible for allocating and freeing space for the table's records in the disk. It also provides a way to access the records in the heap.

## Heap Structure

heap is a collection of pages. Each page is a collection of records. Each record is a collection of fields.

### HeapHeader

- PageCount
- HeapId

## Page Structure

- PageHeader
- Records
- slot array

## Record Structure

- RecordHeader
- Fields

## HeapManager API

HeapManager deals with binary data, so it provides a set of functions to read and write binary data to the disk. so all the functions are dealing with byte arrays, heap manager doesn't know anything about the data types of the fields. It's the responsibility of the higher layers to interpret the data.

as and example, the `InsertRow` function will take a byte slice as a parameter and it will write this byte slice to the heap.

the `GetRow` function will take an identifier and it will return a byte slice that contains the record data.

```go
package main
import (
    "encoding/binary"
    "fmt"
 )

func main () {
    // as example, row contains 2 integers
    x := 10
    y := 20
    row := make([]byte, 8)
    binary.LittleEndian.PutUint32(row, uint32(x))
    binary.LittleEndian.PutUint32(row[4:], uint32(y))

    //row is just a slice of bytes that contains the binary representation of the 2 integers

    // insert the row to the heap
    heapManager.InsertRow(row)

    // get the row from the heap
    result := heapManager.GetRow(1)
    fmt.Println(result) // [10 0 0 0 20 0 0 0]
}

```
