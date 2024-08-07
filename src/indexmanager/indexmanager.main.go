package indexmanager

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"github.com/krasun/fbptree"
)

const (
	// IndexRebuildThreshold = 30
	indexPageSize     = 4096
	indexMetadataSize = 52
	metaDataFileName  = "meta.data"
	indexOrder        = 128
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
	fmt.Println(indexPath)

	if _, err := fbptree.Open(indexPath, fbptree.PageSize(indexPageSize), fbptree.Order(indexOrder)); err != nil {
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
		header := make([]byte, 24)
		copy(header[:20], []byte(tableName))
		binary.BigEndian.PutUint32(header[20:], uint32(0))

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
	header := make([]byte, 24)
	if _, err := metaFile.ReadAt(header, 0); err != nil {
		return fmt.Errorf("error reading the metadata file: %w", err)
	}

	// Get the number of indexes
	indexesCount := binary.BigEndian.Uint32(header[20:])

	// Write the index metadata to the file
	indexMetadataBytes := make([]byte, indexMetadataSize)
	copy(indexMetadataBytes[:20], []byte(indexName))
	copy(indexMetadataBytes[20:40], []byte(ColumnName))
	binary.BigEndian.PutUint32(indexMetadataBytes[40:44], uint32(0))
	binary.BigEndian.PutUint32(indexMetadataBytes[44:48], uint32(0))
	binary.BigEndian.PutUint32(indexMetadataBytes[48:52], uint32(0))

	// Write the index metadata to the file
	if _, err := metaFile.WriteAt(indexMetadataBytes, int64(24+indexesCount*indexMetadataSize)); err != nil {
		return fmt.Errorf("error writing index metadata to metadata file: %w", err)
	}

	// Update the header
	binary.BigEndian.PutUint32(header[20:], indexesCount+1)
	if _, err := metaFile.WriteAt(header, 0); err != nil {
		return fmt.Errorf("error updating header of metadata file: %w", err)
	}

	// Flush changes to disk
	if err := metaFile.Sync(); err != nil {
		return fmt.Errorf("error syncing metadata file: %w", err)
	}

	return nil
}

// the first function to add entry to a specific index of the table
func addEntryToIndex(tableName string, indexName string, key []byte, pageID int32) error {

	//open the index file if it exists
	indexDir := path.Join("indexes", tableName)
	indexPath := path.Join(indexDir, indexName+".data")

	tree, err := fbptree.Open(indexPath, fbptree.PageSize(indexPageSize), fbptree.Order(indexOrder))
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
func AddEntryToTableIndexes(tableName string, keys [][]byte, pageID int32) error {
	indexes, err := GetIndexesMetadata(tableName)

	if err != nil {
		return fmt.Errorf("failed to get indexes metadata: %w", err)
	}

	for i, index := range indexes {
		indexName := string(index[:20])
		indexName = strings.Trim(indexName, "\x00")

		if err := addEntryToIndex(tableName, indexName, keys[i], pageID); err != nil {
			return fmt.Errorf("failed to add entry to index %s: %w", indexName, err)
		}

		keysCount := binary.BigEndian.Uint32(index[48:52])
		keysCount++
		binary.BigEndian.PutUint32(index[48:52], keysCount)
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
	metaFile.WriteAt(flatIndexes, 24)

	// Flush changes to disk
	if err := metaFile.Sync(); err != nil {
		return fmt.Errorf("error syncing metadata file: %w", err)
	}
	return nil
}

// RemoveEntryFromTableIndexes removes an entry from all indexes for a given key.
func RemoveEntryFromTableIndexes(tableName string, keys [][]byte) error {
	indexes, err := GetIndexesMetadata(tableName)

	if err != nil {
		return fmt.Errorf("failed to get indexes metadata: %w", err)
	}

	for i, index := range indexes {
		indexName := string(index[:20])
		indexName = strings.Trim(indexName, "\x00")

		if err := removeEntryFromIndex(tableName, indexName, keys[i]); err != nil {
			return fmt.Errorf("failed to remove entry from index %s: %w", indexName, err)
		}

		updatesCount := binary.BigEndian.Uint32(index[40:44])
		keysCount := binary.BigEndian.Uint32(index[48:52])
		updatesCount++
		keysCount--
		binary.BigEndian.PutUint32(index[40:44], updatesCount)
		binary.BigEndian.PutUint32(index[48:52], keysCount)

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
	if _, err := metaFile.WriteAt(flatIndexesMetadata, 24); err != nil {
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
	tree, err := fbptree.Open(indexPath, fbptree.PageSize(indexPageSize), fbptree.Order(indexOrder))
	if err != nil {
		return fmt.Errorf("failed to open B+ tree %s: %w", indexPath, err)
	}
	defer tree.Close()

	_, _, err = tree.Delete(key)

	if err != nil {
		return fmt.Errorf("failed to delete value: %w", err)
	}

	return nil
}

// SearchIndexEntry searches for an entry in the index for a given key, returning the page id.
func FindIndexEntry(tableName string, indexName string, key []byte) (int32, error) {
	// open the index and search for the key
	indexPath := path.Join("indexes", tableName, indexName+".data")
	tree, err := fbptree.Open(indexPath, fbptree.PageSize(indexPageSize), fbptree.Order(indexOrder))
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
	tree, err := fbptree.Open(indexPath, fbptree.PageSize(indexPageSize), fbptree.Order(indexOrder))
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
		indexes, err := GetIndexesMetadata(tableName)
		if err != nil {
			return fmt.Errorf("failed to get indexes metadata: %w", err)
		}
		for _, idx := range indexes {
			idxName := string(idx[:20])
			idxName = strings.Trim(idxName, "\x00")
			if err := deleteIndex(tableName, idxName); err != nil {
				return fmt.Errorf("failed to delete index %s: %w", idxName, err)
			}
		}
	} else {
		// non-clustered index
		// delete the index
		if err := deleteIndex(tableName, indexName); err != nil {
			return fmt.Errorf("failed to delete index %s: %w", indexName, err)
		}
	}
	return nil

}

// Helper function to delete a specific index
func deleteIndex(tableName, indexName string) error {
	// Delete the index file
	//indexPath := path.Join("indexes", tableName, indexName+".data")

	indexDir := path.Join("indexes", tableName)
	indexPath := path.Join(indexDir, indexName+".data")
	fmt.Println(indexPath)

	if err := os.Remove(indexPath); err != nil {
		return fmt.Errorf("failed to delete index file %s: %w", indexPath, err)
	}

	// Remove the index metadata from the metadata file
	metaDataPath := path.Join("indexes", tableName, metaDataFileName)
	metaFile, err := os.OpenFile(metaDataPath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error opening the metadata file: %w", err)
	}
	defer metaFile.Close()

	// Lock mutex to synchronize access to metadata file

	// Read the indexes metadata
	indexes, err := GetIndexesMetadata(tableName)
	if err != nil {
		return fmt.Errorf("failed to get indexes metadata: %w", err)
	}

	metaFilEMutex.Lock()
	defer metaFilEMutex.Unlock()

	// Find and remove the metadata of the deleted index
	var updatedMetadata []byte
	for _, idx := range indexes {
		if strings.Trim(string(idx[:20]), "\x00") != indexName {
			updatedMetadata = append(updatedMetadata, idx...)
		}
	}

	// Write the updated indexes metadata to the file
	if _, err := metaFile.WriteAt(updatedMetadata, 24); err != nil {
		return fmt.Errorf("error writing updated indexes metadata to metadata file: %w", err)
	}

	// Flush changes to disk
	if err := metaFile.Sync(); err != nil {
		return fmt.Errorf("error syncing metadata file: %w", err)
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
	header := make([]byte, 24)
	if _, err := metaFile.ReadAt(header, 0); err != nil {
		return nil, fmt.Errorf("error reading the metadata file: %w", err)
	}

	// Get the number of indexes
	indexesCount := binary.BigEndian.Uint32(header[20:])
	indexesMetadata := make([][]byte, indexesCount)

	// Read the indexes metadata
	for i := uint32(0); i < indexesCount; i++ {
		indexMetadata := make([]byte, indexMetadataSize)
		if _, err := metaFile.ReadAt(indexMetadata, int64(24+i*indexMetadataSize)); err != nil {
			return nil, fmt.Errorf("error reading index metadata from metadata file: %w", err)
		}
		indexesMetadata[i] = indexMetadata
	}

	return indexesMetadata, nil
}

// update the index metadata
func UpdateIndexMetadata(tableName string, indexName string, indexMetadata []byte) error {
	// Calculate the offset of the index metadata in the metadata file
	indexMetadataOffset := int64(24) + getIndexOffset(tableName, indexName)

	// Open the metadata file
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
	if _, err := metaFile.WriteAt(indexMetadata, indexMetadataOffset); err != nil {
		return fmt.Errorf("error writing index metadata to metadata file: %w", err)
	}

	// Flush changes to disk
	if err := metaFile.Sync(); err != nil {
		return fmt.Errorf("error syncing metadata file: %w", err)
	}

	return nil
}

func getIndexOffset(tableName string, indexName string) int64 {
	// Open the metadata file
	metaDataPath := path.Join("indexes", tableName, metaDataFileName)
	metaFile, err := os.OpenFile(metaDataPath, os.O_RDWR, 0644)
	if err != nil {
		return 0 // Return 0 in case of error
	}
	defer metaFile.Close()

	// Read the header to get the number of indexes
	header := make([]byte, 24)
	if _, err := metaFile.ReadAt(header, 0); err != nil {
		return 0 // Return 0 in case of error
	}

	// Get the number of indexes
	indexesCount := binary.BigEndian.Uint32(header[20:])

	// Calculate the offset of the specified index
	indexOffset := int64(24 + indexesCount*indexMetadataSize)

	// Search for the index in the metadata file
	for i := uint32(0); i < indexesCount; i++ {
		indexMetadata := make([]byte, indexMetadataSize)
		if _, err := metaFile.ReadAt(indexMetadata, indexOffset); err != nil {
			return 0 // Return 0 in case of error
		}
		existingIndexName := string(indexMetadata[:20])
		existingIndexName = strings.Trim(existingIndexName, "\x00")
		if existingIndexName == indexName {
			return indexOffset // Return the offset of the index
		}
		indexOffset += indexMetadataSize
	}

	return 0 // Return 0 if the index is not found
}



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

// func GetIndexHeight(tableName string, indexName string) (int32, error) {
// 	return 0, nil
// 	/*
// 		check if the index exists
// 		return the height of the index
// 	*/
// }

// [-- TO BE IMPLEMENTED --]
// func CheckIndexRebuild(tableName string) (string, error) {

// 	/* read the indexes meta to know all the indexes for the table
// 	     iterate over all indexes and check if the index needs to be rebuilt
// 		 if the index needs to be rebuilt, call the rebuild index function

// 		 This fucntion to be called after each update or delete operation happens on the table ,

// 		 --
// 		NOte : i know it is not the best solution to call this function after each update or delete operation
// 				but it is the best solution for the first submission

// 		 --
// 	*/

// 	return "", nil
// }

// [-- TO BE IMPLEMENTED --]
// func RebuildIndex(tableName string, indexName string) error {
// 	return nil

// 	/*
// 		check if the index exists
// 		rebuild the index by creating a new B+ tree and adding the entries from the table or reblancing the tree
// 	*/
// }
