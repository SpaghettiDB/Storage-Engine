package indexmanager



const IndexRebuildThreshold = 30 // 30% of the index is deleted or updated


// InitializeIndex initializes an index for a given table with a specified name.
// It should create necessary data structures for the index.
// This function can handle both clustered and non-clustered indexes.
func InitializeIndex(tableName string, indexName string, clustered bool) error {
	return nil


/*
check if the index already exists or table is not present
check if clustered or non-clustered
if clustered, create a B+ tree with the key as the primary key
if non-clustered, create a B+ tree with the key as the index key , scan the table and add the index entries
*/
}

// AddIndexEntry adds an entry to the index for a given key and page ID.
// It should handle both clustered and non-clustered indexes.
func AddIndexEntry(tableName string, indexName string, key []byte, pageID int32) error {
	return nil

	/*
		check if the index exists
	    add the key to the B+ tree

	*/

}

// RemoveIndexEntry removes an entry from the index for a given key.
// It should handle both clustered and non-clustered indexes.
func RemoveIndexEntry(tableName string, indexName string, key []byte) error {
	return nil
	/*
		check if the index exists
		remove the key from the B+ tree
	*/
}

// SearchIndexEntry searches for an entry in the index for a given key.
// It should return the corresponding page ID associated with the key.
func FindIndexEntry(tableName string, indexName string, key []byte) (int32, error) {
	return 0, nil

	/*
		check if the index exists
		search the key in the B+ tree and return the page ID 
	*/
}

// UpdateIndexEntry updates an entry in the index for a given key with a new page ID.
// It should handle both clustered and non-clustered indexes.
func UpdateIndexEntry(tableName string, indexName string, key []byte, newPageID int32) error {
	return nil

	/*
		check if the index exists
		update the key in the B+ tree with the new page ID

	*/
}

// ScanIndexRange scans the index for entries within a specified key range.
// It should return a list of page IDs corresponding to keys within the range.
func ScanIndexRange(tableName string, indexName string, startKey []byte, endKey []byte) ([]int32, error) {
	return nil, nil

	/*
		check if the index exists
		scan the B+ tree for the keys within the range and return the page IDs respectively
	*/

}

//DeleteIndex deletes the index for a given table and index name.
//It should remove any data structures associated with the index.
func DeleteIndex(tableName string, indexName string) error {
	return nil
	/*
		check if the index exists
		delete the index
	*/
}



/* These functions are not required for the first submission
   but they are good to have for future optimizations in execution plan and calculation of query cost
*/





// GetIndexSize returns the size of the index in bytes.
// It should return the size of the index data structures.
// It can be used for optimization or after significant data chanes. 
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

func CheckIndexRebuild(tableName string, indexName string) (bool, error) {
	return false, nil
	/*
		check if the index exists
		check if the index needs to be rebuilt based on the threshold by checking the number of deleted or updated entries	
	*/
}	


// GetIndexEntriesCount returns the number of entries in the index.
func GetIndexEntriesCount(tableName string, indexName string) (int64, error) {
	return 0, nil
	/*
		check if the index exists
		return the number of entries in the index
	*/
}

// count updated and deleted entries
func CountUpdatedDeletedEntries(tableName string, indexName string) (int32, error) {
	return 0, nil
	/*
		check if the index exists
		return the number of updated and deleted entries in the index
	*/
}

// RebuildIndex rebuilds the index for a given table and index name.
// It can be used for optimization or after significant data changes.
func RebuildIndex(tableName string, indexName string) error {
	return nil

	/*
		check if the index exists
		rebuild the index by creating a new B+ tree and adding the entries from the table or reblancing the tree
	*/
}
