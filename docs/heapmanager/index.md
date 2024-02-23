## Index Documentation

This is the index documentation for the indexmanger module. This module is responsible for managing the index of the database. The index is a data structure that is used to optimize the retrieval of data from the database. The index manager module is responsible for creating, updating, and deleting the index. It also provides functions to search for data in the index.

The index manager module is implemented using the B+ tree data structure. The B+ tree is a self-balancing tree data structure that is commonly used in database systems. It is designed to optimize the retrieval of data by minimizing the number of disk accesses required to find a particular record.

## Index Metadata Structure

The index manager module maintains metadata for all indexes in a file with the following path: `indexes/Table_Name/meta.data`. The metadata includes information such as the name of the index, the data type of the key, and the file offset of the root node of the B+ tree. This metadata is stored in a system catalog table, which is used to keep track of all the indexes in the database.

[Table Name Length (20 bytes)][Number of Indexes (4 bytes)]

[Index 1]
[Index 2]
...
[Index N]

[Index Structure]

- Index Name (20 bytes)
- Column Name (20 bytes)
- updatesCount (4 bytes)
- Indexversion (4 bytes)
- number of keys (4 bytes)
  ...

## Index Structure

since the index manager module uses the B+ tree data structure to store the index, the index is a tree with the following path: `indexes/Table_Name/Index_Name.data`.

## code of conduct

- A new index is initialized in two cases a new table is created or a new index is created throughout a query.
- when a new row is inserted or deleted, all the indexes found in that table should updates their own date with the appropriate operation.
- when the table is deleted, all the indexes found in that table should be deleted as well.
- when the index is deleted, the index file should be deleted and the metadata should be updated.
- when any operation is performed on the index, the metadata should be updated.
- when the index is created or updated, the metadata should be updated.
