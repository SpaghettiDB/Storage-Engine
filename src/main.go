package main

import (
	"github.com/SpaghettiDB/Storage-Engine/src/schemamanager"
	"fmt"
)

func main() {
	// HeapManager PlayGround ----------------------------------------------------------------

	// heapmanager.PlayGround()

	//IndexManager-------------------------------------------------------------------

	// indexmanager.PlayGround()

	// indexmanager.InitializeIndex("test", "test", "test", false)

	//scan test -------------------------------------------------------------------

	// err := indexmanager.InitializeIndex("Student", "name", "name", false)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// err = indexmanager.InitializeIndex("Student", "id", "id", false)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// d := make([]byte, 4)
	// binary.BigEndian.PutUint32(d, 2)

	// indexmanager.AddEntryToTableIndexes("Student", [][]byte{[]byte("mohammed"), d}, 2)

	// result, err := indexmanager.GetIndexSize("Student", "name")

	// if err != nil {
	// 	fmt.Println(err)
	// }

	// fmt.Println(result)




	//schema testing -------------------------------------------------




	// err := schemamanager.AddTable(schemamanager.Table{
	// 	Name: "Student", 
	// })

	// if err != nil {
	// 	panic(err)
	// }

	// err = schemamanager.AddColumn("Student", schemamanager.Column{
	// 	Name: "id",
	// 	DataType: "int",
	// })

	// if err != nil {
	// 	panic(err)
	// }
	// err = schemamanager.AddIndex("Student", schemamanager.Index{
	// 	Name: "idIndex",
	// 	ColumnName: "id",
	// })

	// if err != nil {
	// 	panic(err)
	// }

	schemamap , err := schemamanager.GetSchemaMap()

	if err != nil {
		panic(err)
	}

	fmt.Println(schemamap)

}
