package indexmanager

const IndexRebuildThreshold = 30 // 30% of the index is deleted or updated

// InitializeIndex creates a new index for a given table and index name.
// init the index metadata and data structures
func InitializeIndex(tableName string, indexName string, clustered bool) error {

	// create the index metadata
	// create the index data structures
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

func AddIndexEntry(tableName string, indexName string, key []byte, pageID int32) error {

	// read the index metadata to know all the indexes for the table
	if indexName == "" {
		// clustered index
		// iterate over all indexes and add the key to its B+ tree
	} else {
		// non-clustered index
		// add the key to the B+ tree of this specific index
	}

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

//DeleteIndex deletes the index for a given table, following the same logic of the add index entry function
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
