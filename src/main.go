package main

import (
	"fmt"
	"os"
)

func main() {

	//create new file
	file, err := os.Create("test.txt")
	if err != nil {
		fmt.Println(err)
	}

	//write to file
	_, err = file.WriteString("Hello World")
	if err != nil {
		fmt.Println(err)
	}

	//write to the end of the file

	//convert string to byte slice
	byteSlice := []byte("Appending to existing file")
	_, err = file.Write(byteSlice)
	if err != nil {
		fmt.Println(err)
	}

	file.Sync() //flush the file to ensure durability

	//get file size
	fileStat, err := file.Stat()
	if err != nil {
		fmt.Println(err)
	}

	//make a byte slice
	fileSize := fileStat.Size()
	byteSlice = make([]byte, fileSize)

	//read file
	_, err = file.ReadAt(byteSlice, 0)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(string(byteSlice))

}
