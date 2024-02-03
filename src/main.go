package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

func main() {

	// make variable x of type int32
	var x int64 = -400000

	//conver x to byte slice
	bs := make([]byte, 8)
	binary.PutVarint(bs, x)

	// open file test.data
	file, err := os.Create("test.data")
	if err != nil {
		fmt.Println(err)
		return
	}

	// write byte slice to file
	_, err = file.Write(bs)
	if err != nil {
		fmt.Println(err)
		return
	}

	// close file
	file.Close()

	// open file test.data
	file, err = os.Open("test.data")
	if err != nil {
		fmt.Println(err)
		return
	}

	// read byte slice from file
	bs2 := make([]byte, 8)
	_, err = file.Read(bs2)
	if err != nil {
		fmt.Println(err)
		return
	}

	// pring byte slice
	fmt.Println(bs2)

	// convert byte slice to int64
	x2, _ := binary.Varint(bs2)

	// print int32
	fmt.Println(x2)

	// close file
	file.Close()

}
