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
	ColumnName []string `json:"columnName"`
}

type Table struct{
	Name string `json:"name"`
	Columns []Column `json:"columns"`
	Indexes []Index `json:"indexes"`
}



type Schema struct{
	Tables []Table `json:"tables"`
}

func GetTables () ([]Table , error) {
	//read the schema json file from the current directory ./schema.json
	//if file does not exist return empty array
	//if file exists read the json file and return the tables array
	//open the file
	schemaPath := path.Join("schemamanager", "schema.json")
	file, err := os.Open(schemaPath)
	if err != nil {
		return nil , fmt.Errorf("Error opening file schema.json : %v", err)
	}
	defer file.Close()
	
	
	//read the file
	decoder := json.NewDecoder(file)
	var schema Schema
	err = decoder.Decode(&schema)

	if err != nil {
		return nil , fmt.Errorf("Error decoding json file : %v", err)
	}
	return schema.Tables , nil
}

func getColumns (table string) ([]string , error) {
	//read from schema file
	return nil , nil
}

func getIndexes (table string) []string {
	//read from schema file
	return nil 
}



func AddTable (table Table) error {
	

	//read the schema json file from the current directory ./schema.json
	//if file does not exist create a new file and write the table to it
	//if file exists read the json file and append the table to the tables array
	//open the file

	schemaPath := path.Join("schemamanager", "schema.json")
	file, err := os.OpenFile( schemaPath , os.O_RDWR , 0644)
	if err != nil {
		//create a new file
		file, err = os.Create(schemaPath)
		if err != nil {
			return fmt.Errorf("Error creating file schema.json : %v", err)
		}
	}
	defer file.Close()

	//read the file
	decoder := json.NewDecoder(file)
	var schema Schema
	err = decoder.Decode(&schema)

	if err != nil {
		return fmt.Errorf("Error decoding json file : %v", err)
	}

	//append the table to the tables array
	schema.Tables = append(schema.Tables, table)

	//write the schema back to the file with overriding the whole file content 
	
	encoder := json.NewEncoder(file)

	_, err = file.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("Error seeking file schema.json : %v", err)
	}
	err = encoder.Encode(schema)
	if err != nil {
		return fmt.Errorf("Error encoding json file : %v", err)
	}
	return nil
}



func AddColumn (table string , column Column) error {
	

	//read the schema json file from the current directory ./schema.json
	//if file does not exist create a new file and write the table to it
	//if file exists read the json file and append the table to the tables array
	//open the file

	schemaPath := path.Join("schemamanager", "schema.json")
	file, err := os.OpenFile( schemaPath , os.O_RDWR , 0644)
	if err != nil {
		//create a new file
		file, err = os.Create(schemaPath)
		if err != nil {
			return fmt.Errorf("Error creating file schema.json : %v", err)
		}
	}
	defer file.Close()


	//read the file
	decoder := json.NewDecoder(file)
	var schema Schema
	err = decoder.Decode(&schema)

	if err != nil {
		return fmt.Errorf("Error decoding json file : %v", err)
	}

	//find the table
	var tableIndex int
	for i , t := range schema.Tables {
		if t.Name == table {
			tableIndex = i
			break
		}
	}

	//append the column to the columns array
	schema.Tables[tableIndex].Columns = append(schema.Tables[tableIndex].Columns, column)

	//write the schema back to the file with overriding the whole file content
	encoder := json.NewEncoder(file)

	_, err = file.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("Error seeking file schema.json : %v", err)
	}
	err = encoder.Encode(schema)
	if err != nil {
		return fmt.Errorf("Error encoding json file : %v", err)
	}
	return nil
}

