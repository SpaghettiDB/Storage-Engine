package indexmanager

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/krasun/fbptree"
)

const (
	IndexRebuildThreshold = 30
	indexPageSize         = 4096
	indexMetadataSize     = 20
	metaDataFileName      = "meta.data"
)

var metaFilEMutex sync.Mutex

// InitializeIndex creates a new index for a given table and index name.
// init the index metadata and data structures
func InitializeIndex(tableName string, indexName string, ColumnName string, clustered bool) error {
	// Construct index directory path
	indexDir := path.Join("indexes", tableName)

	// Create index directory if it doesn't exist
	if _, err := os.Stat(indexDir); os.IsNotExist(err) {
		if err := os.MkdirAll(indexDir, os.ModePerm); err != nil {
			return fmt.Errorf("failed to create index directory: %w", err)
		}
	}

	// Create index file
	indexPath := path.Join(indexDir, indexName+".data")
	if _, err := fbptree.Open(indexPath, fbptree.PageSize(4096), fbptree.Order(500)); err != nil {
		return fmt.Errorf("failed to open B+ tree %s: %w", indexPath, err)
	}

	// Check if the metadata file exists, if not, it is a clustered index
	metaDataPath := path.Join(indexDir, metaDataFileName)
	if _, err := os.Stat(metaDataPath); os.IsNotExist(err) {
		metaFile, err := os.Create(metaDataPath)
		if err != nil {
			return fmt.Errorf("failed to create metadata file: %w", err)
		}
		defer metaFile.Close()

		// Write table name and index number to the header
		header := make([]byte, 8)
		copy(header[:4], []byte(tableName))
		binary.BigEndian.PutUint32(header[4:], uint32(0))

		// Write the header to the file
		if _, err := metaFile.WriteAt(header, 0); err != nil {
			return fmt.Errorf("error writing header to metadata file: %w", err)
		}
	}

	// Open the metadata file
	metaFile, err := os.OpenFile(metaDataPath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error opening the metadata file: %w", err)
	}
	defer metaFile.Close()

	// Lock mutex to synchronize access to metadata file
	metaFilEMutex.Lock()
	defer metaFilEMutex.Unlock()

	// Read the header
	header := make([]byte, 8)
	if _, err := metaFile.ReadAt(header, 0); err != nil {
		return fmt.Errorf("error reading the metadata file: %w", err)
	}

	// Get the number of indexes
	indexesCount := binary.BigEndian.Uint32(header[4:])

	// Write the index metadata to the file
	indexMetadataBytes := make([]byte, indexMetadataSize)
	copy(indexMetadataBytes[:4], []byte(indexName))
	copy(indexMetadataBytes[4:8], []byte(ColumnName))
	binary.BigEndian.PutUint32(indexMetadataBytes[8:], uint32(0))
	binary.BigEndian.PutUint32(indexMetadataBytes[12:], uint32(0))
	binary.BigEndian.PutUint32(indexMetadataBytes[16:], uint32(0))

	// Write the index metadata to the file
	if _, err := metaFile.WriteAt(indexMetadataBytes, int64(8+indexesCount*indexMetadataSize)); err != nil {
		return fmt.Errorf("error writing index metadata to metadata file: %w", err)
	}

	// Update the header
	binary.BigEndian.PutUint32(header[4:], indexesCount+1)
	if _, err := metaFile.WriteAt(header, 0); err != nil {
		return fmt.Errorf("error updating header of metadata file: %w", err)
	}

	// Flush changes to disk
	if err := metaFile.Sync(); err != nil {
		return fmt.Errorf("error syncing metadata file: %w", err)
	}

	return nil
}

/*
There is two cases for the add index entry function
1- clustered index or prefound index
  your dont need the index name , you just need the table name and the key and the page ID
and you will iterate over all indexes and add the key to its B+ tree

2- non-clustered index
  the table is already has a data in this cloumn do you need to scan data
   and add the key to the B+ tree of this specific index
*/

// the first function to add entry to a specific index of the table
func addEntryToIndex(tableName string, indexName string, key []byte, pageID int32) error {

	//open the index file if it exists
	indexPath := path.Join("indexes", tableName, indexName+".data")
	tree, err := fbptree.Open(indexPath, fbptree.PageSize(4096), fbptree.Order(500))
	if err != nil {
		return fmt.Errorf("failed to open B+ tree %s: %w", indexPath, err)
	}
	defer tree.Close()

	// add the key to the index
	//make the page id array of three bytes using big endian
	pageIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(pageIDBytes, uint32(pageID))

	//get the key first to check if it exists
	_, ok, err := tree.Get(key)
	if err != nil {
		return fmt.Errorf("failed to get value: %w", err)
	}

	if !ok {
		if _, _, err := tree.Put(key, pageIDBytes); err != nil {
			return fmt.Errorf("failed to insert value: %w", err)
		}

	} else {
		return fmt.Errorf("the key already exists in the index")
	}

	return nil
}

// the second function to add entry to all indexes of the table
func AddEntryToTableIndexes(tableName string, key []byte, pageID int32) error {
	indexes, err := GetIndexesMetadata(tableName)

	if err != nil {
		return fmt.Errorf("failed to get indexes metadata: %w", err)
	}

	for _, index := range indexes {
		indexName := string(index[:4])
		if err := addEntryToIndex(tableName, indexName, key, pageID); err != nil {
			return fmt.Errorf("failed to add entry to index %s: %w", indexName, err)
		}

		keysCount := binary.BigEndian.Uint32(index[16:])
		keysCount++
		binary.BigEndian.PutUint32(index[16:], keysCount)
	}

	// update the index metadata
	// open the metadata file
	metaDataPath := path.Join("indexes", tableName, metaDataFileName)
	metaFile, err := os.OpenFile(metaDataPath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error opening the metadata file: %w", err)
	}
	defer metaFile.Close()

	// Lock mutex to synchronize access to metadata file
	metaFilEMutex.Lock()
	defer metaFilEMutex.Unlock()

	// Write the index metadata to the file

	//flatten the indexes array
	flatIndexes := make([]byte, 0)
	for _, index := range indexes {
		flatIndexes = append(flatIndexes, index...)
	}
	metaFile.WriteAt(flatIndexes, 8)
	return nil
}

// RemoveEntryFromTableIndexes removes an entry from all indexes for a given key.
func RemoveEntryFromTableIndexes(tableName string, key []byte) error {
	indexes, err := GetIndexesMetadata(tableName)
	if err != nil {
		return fmt.Errorf("failed to get indexes metadata: %w", err)
	}

	for i, index := range indexes {
		indexName := string(index[:4])
		if err := removeEntryFromIndex(tableName, indexName, key); err != nil {
			return fmt.Errorf("failed to remove entry from index %s: %w", indexName, err)
		}

		updatesCount := binary.BigEndian.Uint32(index[8:])
		keysCount := binary.BigEndian.Uint32(index[16:])
		updatesCount++
		keysCount--
		binary.BigEndian.PutUint32(index[8:], updatesCount)
		binary.BigEndian.PutUint32(index[16:], keysCount)

		indexes[i] = index
	}

	indexDir := path.Join("indexes", tableName)
	metaDataPath := path.Join(indexDir, metaDataFileName)

	// Open the metadata file
	metaFile, err := os.OpenFile(metaDataPath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error opening the metadata file: %w", err)
	}
	defer metaFile.Close()

	// flat the indexes metadata
	flatIndexesMetadata := make([]byte, 0)
	for _, index := range indexes {
		flatIndexesMetadata = append(flatIndexesMetadata, index...)
	}

	// Lock mutex to synchronize access to metadata file
	metaFilEMutex.Lock()
	defer metaFilEMutex.Unlock()

	// Write the indexes metadata to the file
	if _, err := metaFile.WriteAt(flatIndexesMetadata, 8); err != nil {
		return fmt.Errorf("error writing indexes metadata to metadata file: %w", err)
	}

	// Flush changes to disk
	if err := metaFile.Sync(); err != nil {
		return fmt.Errorf("error syncing metadata file: %w", err)
	}

	return nil
}

// RemoveEntryFromIndex removes an entry from a specific index for a given key.
func removeEntryFromIndex(tableName string, indexName string, key []byte) error {
	indexPath := path.Join("indexes", tableName, indexName+".data")
	tree, err := fbptree.Open(indexPath, fbptree.PageSize(4096), fbptree.Order(500))
	if err != nil {
		return fmt.Errorf("failed to open B+ tree %s: %w", indexPath, err)
	}
	defer tree.Close()

	_, ok, err := tree.Delete(key)
	if err != nil {
		return fmt.Errorf("failed to delete value: %w", err)
	}
	if !ok {
		return fmt.Errorf("failed to find value to delete")
	}
	return nil
}

// SearchIndexEntry searches for an entry in the index for a given key, returning the page id.
func FindIndexEntry(tableName string, indexName string, key []byte) (int32, error) {
	// open the index and search for the key
	indexPath := path.Join("indexes", tableName, indexName+".data")
	tree, err := fbptree.Open(indexPath, fbptree.PageSize(4096), fbptree.Order(500))
	if err != nil {
		return 0, fmt.Errorf("failed to open B+ tree %s: %w", indexPath, err)
	}

	defer tree.Close()
	// search for the key
	PageID, ok, err := tree.Get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get value: %w", err)
	}
	if !ok {
		return 0, fmt.Errorf("failed to find value")
	}

	return int32(binary.BigEndian.Uint32(PageID)), nil
}

// ScanIndexRange scans the index for entries within a specified key range, returning a list of page IDs corresponding to keys within the range.
func ScanIndexRange(tableName string, indexName string, startKey []byte, endKey []byte) ([]int32, error) {
	//convert the start and end key to int32
	// open the index and scan the range
	indexPath := path.Join("indexes", tableName, indexName+".data")
	tree, err := fbptree.Open(indexPath, fbptree.PageSize(4096), fbptree.Order(500))
	if err != nil {
		return nil, fmt.Errorf("failed to open B+ tree %s: %w", indexPath, err)
	}
	defer tree.Close()

	startKeyInt32 := binary.BigEndian.Uint32(startKey)
	endKeyInt32 := binary.BigEndian.Uint32(endKey)

	//loop from start to end and get the key from the index

	result := make([]int32, 0)
	for i := startKeyInt32; i <= endKeyInt32; i++ {

		//convert i to byte array
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, i)

		pageId, ok, err := tree.Get(key)
		if err != nil {
			return nil, fmt.Errorf("failed to get value: %w", err)
		}
		if ok {
			//convert the page id to int32
			result = append(result, int32(binary.BigEndian.Uint32(pageId)))
		}
	}

	return result, nil
	// open the index and scan the range
}

// DeleteIndex deletes the index for a given table, following the same logic of the add index entry function
func DeleteIndex(tableName string, indexName string) error {

	// read the indexes meta to know all the indexes for the table
	if indexName == "" {
		// clustered index
		// iterate over all indexes and delete the index
	} else {
		// non-clustered index
		// delete the index
	}
	return nil

}

// GetIndexMetadata returns the metadata for a given table.
func GetIndexesMetadata(tableName string) ([][]byte, error) {
	// read the indexes meta to know all the indexes for the table
	indexDir := path.Join("indexes", tableName)
	metaDataPath := path.Join(indexDir, metaDataFileName)

	// Open the metadata file
	metaFile, err := os.OpenFile(metaDataPath, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening the metadata file: %w", err)
	}
	defer metaFile.Close()

	// Lock mutex to synchronize access to metadata file
	metaFilEMutex.Lock()
	defer metaFilEMutex.Unlock()

	// Read the header
	header := make([]byte, 8)
	if _, err := metaFile.ReadAt(header, 0); err != nil {
		return nil, fmt.Errorf("error reading the metadata file: %w", err)
	}

	// Get the number of indexes
	indexesCount := binary.BigEndian.Uint32(header[4:])
	indexesMetadata := make([][]byte, indexesCount)

	// Read the indexes metadata
	for i := uint32(0); i < indexesCount; i++ {
		indexMetadata := make([]byte, indexMetadataSize)
		if _, err := metaFile.ReadAt(indexMetadata, int64(8+i*indexMetadataSize)); err != nil {
			return nil, fmt.Errorf("error reading index metadata from metadata file: %w", err)
		}
		indexesMetadata[i] = indexMetadata
	}

	return indexesMetadata, nil
}

// update the index metadata
func UpdateIndexMetadata(tableName string, indexName string, indexMetadata []byte) error {
	return nil
}

/*
 	These functions are not required for the first submission
    but they are good to have for future optimizations in execution plan and calculation of query cost
*/

// GetIndexSize returns the size of the index in bytes.
// It should return the size of the index data structures.
func GetIndexSize(tableName string, indexName string) (int32, error) {
	indexPath := path.Join("indexes", tableName, indexName+".data")
	fileInfo, err := os.Stat(indexPath)
	if err != nil {
		return 0, fmt.Errorf("failed to get index size: %w", err)
	}
	return int32(fileInfo.Size()), nil
}

// GetIndexHeight returns the height of the index.
func GetIndexHeight(tableName string, indexName string) (int32, error) {
	return 0, nil
	/*
		check if the index exists
		return the height of the index
	*/
}

func CheckIndexRebuild(tableName string) (string, error) {

	/* read the indexes meta to know all the indexes for the table
	     iterate over all indexes and check if the index needs to be rebuilt
		 if the index needs to be rebuilt, call the rebuild index function

		 This fucntion to be called after each update or delete operation happens on the table ,

		 --
		NOte : i know it is not the best solution to call this function after each update or delete operation
				but it is the best solution for the first submission

		 --
	*/

	return "", nil
}

// [-- TO BE IMPLEMENTED --]
func RebuildIndex(tableName string, indexName string) error {
	return nil

	/*
		check if the index exists
		rebuild the index by creating a new B+ tree and adding the entries from the table or reblancing the tree
	*/
}
