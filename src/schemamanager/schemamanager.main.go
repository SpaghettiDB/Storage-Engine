//this is schemamanager package main file this module is responsible
//for storing data about the schema of the database like tables cols and indexes etc
//for sake of simplicity we will use json file to store the schema data

package schemamanager

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
)



 type  Column struct{
	Name string `json:"name"`
	DataType string `json:"dataType"`
}

type Index struct{
	Name string `json:"name"`
	ColumnName string `json:"columnName"`
}

type Table struct{
	Name string `json:"name"`
	Columns []Column `json:"columns"`
	Indexes []Index `json:"indexes"`
}



type Schema struct{
	Tables []Table `json:"tables"`
}



//function that returns a map of key:table name , value:columns names array
func GetSchemaMap 	() (map[string][]string , error) {
	schemaPath := path.Join("schemamanager", "schema.json")
	file, err := os.Open(schemaPath)
	if err != nil {
		return nil , fmt.Errorf("%v", err)
	}
	defer file.Close()
	
	
	//read the file
	decoder := json.NewDecoder(file)
	var schema Schema
	err = decoder.Decode(&schema)

	if err != nil {
		return nil , fmt.Errorf("%v", err)
	}

	schemaMap := make(map[string][]string)
	for _, table := range schema.Tables {
		columns := make([]string, 0)
		for _, col := range table.Columns {
			columns = append(columns, col.Name)
		}
		schemaMap[table.Name] = columns
	}
	return schemaMap , nil
}



func GetTables () ([]Table , error) {
	schemaPath := path.Join("schemamanager", "schema.json")
	file, err := os.Open(schemaPath)
	if err != nil {
		return nil , fmt.Errorf("%v", err)
	}
	defer file.Close()
	
	
	//read the file
	decoder := json.NewDecoder(file)
	var schema Schema
	err = decoder.Decode(&schema)

	if err != nil {
		return nil , fmt.Errorf("%v", err)
	}
	return schema.Tables , nil
}




func AddTable (table Table) error {
	
	file , err := openSchemaFile()
	if err != nil {
		return err
	}

	decoder := json.NewDecoder(file)
	var schema Schema
	err = decoder.Decode(&schema)

	if err != nil {
		return fmt.Errorf(" %v", err)
	}

	file.Close()


	//check if the table already exists
	for _, t := range schema.Tables {
		if t.Name == table.Name {
			return fmt.Errorf("TABLE ALREADY EXISTS")
		}
	}

	//append the table to the tables array
	schema.Tables = append(schema.Tables, table)

	//open the file with truncating the content of the file
	file, err = openSchemaFileWithTruncate()
	if err != nil {
		return err
	}


	defer file.Close()
	encoder := json.NewEncoder(file)
	err = encoder.Encode(schema)
	if err != nil {
		return fmt.Errorf("%v", err)
	}
	return nil
}



func AddColumn (table string , column Column) error {

	file , err := openSchemaFile()
	if err != nil {
		return err
	}


	//read the file
	decoder := json.NewDecoder(file)
	var schema Schema
	err = decoder.Decode(&schema)

	if err != nil {
		return fmt.Errorf("%v", err)
	}

	file.Close()	

	//find the table
	var tableIndex int = -1
	for i , t := range schema.Tables {
		if t.Name == table {
			tableIndex = i
			break
		}
	}

	if tableIndex == -1 {
		return fmt.Errorf("Table not found")
	}

	//check if the column already exists
	for _, c := range schema.Tables[tableIndex].Columns {
		if c.Name == column.Name {
			return fmt.Errorf("COLUMN ALREADY EXISTS")
		}
	}

	//append the column to the columns array
	schema.Tables[tableIndex].Columns = append(schema.Tables[tableIndex].Columns, column)
	
	//open the file with truncating the content of the file
	file, err = openSchemaFileWithTruncate()

	if err != nil {
		return err
	}

	//write the schema back to the file with overriding the whole file content
	encoder := json.NewEncoder(file)

	err = encoder.Encode(schema)
	if err != nil {
		return fmt.Errorf("%v", err)
	}
	return nil
}




func AddIndex (table string , index Index) error {
	file , err := openSchemaFile()

	if err != nil {
		return err
	}

	decoder := json.NewDecoder(file)
	var schema Schema
	err = decoder.Decode(&schema)
	file.Close()

	if err != nil {
		return fmt.Errorf("%v", err)
	}

	var tableIndex int = -1
	for i , t := range schema.Tables {
		if t.Name == table {
			tableIndex = i
			break
		}
	}

	if tableIndex == -1 {
		return fmt.Errorf("TABLE NOT FOUND")
	}


	//check if the index already exists
	for _, i := range schema.Tables[tableIndex].Indexes {
		if i.Name == index.Name {
			return fmt.Errorf("INDEX ALREADY EXISTS")
		}
	}

	//append the index to the indexes array
	schema.Tables[tableIndex].Indexes = append(schema.Tables[tableIndex].Indexes, index)

	//open the file with truncating the content of the file
	file, err = openSchemaFileWithTruncate()
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(file)
	err = encoder.Encode(schema)
	if err != nil {
		return fmt.Errorf("%v", err)
	}
	return nil
}





//helper function to open the schema file
func openSchemaFile() (*os.File , error) {
	schemaPath := path.Join("schemamanager", "schema.json")
	file, err := os.OpenFile( schemaPath , os.O_RDWR , 0644)
	if err != nil {
		//create a new file
		file, err = os.Create(schemaPath)
		if err != nil {
			return nil , fmt.Errorf("%v", err)
		}
	}
	return file , nil
}



//helper function to open the schema file with truncating the content of the file
func openSchemaFileWithTruncate() (*os.File , error) {
	schemaPath := path.Join("schemamanager", "schema.json")
	file, err := os.OpenFile( schemaPath , os.O_RDWR | os.O_TRUNC , 0644)
	if err != nil {
		//create a new file
		file, err = os.Create(schemaPath)
		if err != nil {
			return nil , fmt.Errorf("%v", err)
		}
	}
	return file , nil
}