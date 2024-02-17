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
func AddEntryToIndex(tableName string, indexName string, key []byte, pageID int32) error {

	return nil
}

// the second function to add entry to all indexes of the table
func AddEntryToTableIndexes(tableName string, key []byte, pageID int32) error {

	return nil
}

// RemoveIndexEntry removes an entry from the index for a given key.
// same as the add index entry function
func RemoveIndexEntry(tableName string, indexName string, key []byte) error {

	// read the indexes meta to know all the indexes for the table
	if indexName == "" {
		// clustered index
		// iterate over all indexes and remove the key from its B+ tree
	} else {
		// non-clustered index
		// remove the key from the B+ tree of this specific index
	}
	return nil
}

// SearchIndexEntry searches for an entry in the index for a given key, returning the page id.
func FindIndexEntry(tableName string, indexName string, key []byte) (int32, error) {
	// reacch the index path and open the tree then search for the key
	return 0, nil
}

// ScanIndexRange scans the index for entries within a specified key range, returning a list of page IDs corresponding to keys within the range.
func ScanIndexRange(tableName string, indexName string, startKey []byte, endKey []byte) ([]int32, error) {
	return nil, nil
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
	return nil, nil
	/*
		return the indexes metadata for the table
	*/
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
	return 0, nil
	/*
		check if the index exists
		return the size of the index
	*/
}

// GetIndexHeight returns the height of the index.
func GetIndexHeight(tableName string, indexName string) (int32, error) {
	return 0, nil
	/*
		check if the index exists
		return the height of the index
	*/
}

// now we wanna optimize the index when it reach a certain case that we need to rebuild it
// we can use the following function to rebuild the index

func CheckIndexRebuild(tableName string) error {

	/* read the indexes meta to know all the indexes for the table
	     iterate over all indexes and check if the index needs to be rebuilt
		 if the index needs to be rebuilt, call the rebuild index function

		 This fucntion to be called after each update or delete operation happens on the table ,

		 --
		NOte : i know it is not the best solution to call this function after each update or delete operation
				but it is the best solution for the first submission

		 --
	*/

	return nil
}

// // GetIndexEntriesCount returns the number of entries in the index.
// func GetIndexEntriesCount(tableName string, indexName string) (int64, error) {
// 	return 0, nil
// 	/*
// 		check if the index exists
// 		return the number of entries in the index
// 	*/
// }

// // count updated and deleted entries
// func CountUpdatedDeletedEntries(tableName string, indexName string) (int32, error) {
// 	return 0, nil
// 	/*
// 		check if the index exists
// 		return the number of updated and deleted entries in the index
// 	*/
// }

// RebuildIndex rebuilds the index for a given table and index name.
// It can be used for optimization or after significant data changes.
func RebuildIndex(tableName string, indexName string) error {
	return nil

	/*
		check if the index exists
		rebuild the index by creating a new B+ tree and adding the entries from the table or reblancing the tree
	*/
}
