# Database Storage Engine Documentation

Welcome to the documentation for our database storage engine! This storage engine consists of two main components: the HeapManager and the IndexManager. Below, we'll provide an overview of each component along with their functionalities.

## HeapManager

The HeapManager is responsible for managing the heap of each table in the database. It handles the allocation and freeing of space for table records on disk. Here's a brief overview of its structure and functionality:

### Heap Structure

The heap is organized into pages, where each page contains a collection of records. The key components of the heap structure include:

- **HeapHeader**: Contains metadata about the heap such as the number of pages and the total number of rows.
- **Page Structure**: Each page contains a header and a collection of records.
- **Record Structure**: Describes the structure of each record within a page.
- **SlotArray**: An array containing information about each record's offset and size within the page.

### HeapManager API

The HeapManager provides a set of functions for interacting with the heap. Notably, it deals with binary data, allowing users to insert and retrieve rows in the form of byte slices. Here are some of the key functions:

- `CreateHeap(name string)`: Creates a new heap file and initializes its header.
- `AddRowToHeap(name string, row []byte)`: Adds a new row to the specified heap.
- `GetRowFromHeap(name string, rowIndex int) []byte`: Retrieves a row from the heap based on its index.
- `GetPageFromHeap(name string, pageIndex int) [][]byte`: Retrieves all records from a specific page in the heap.

## IndexManager

The IndexManager is responsible for managing indexes in the database. It utilizes the B+ tree data structure to optimize data retrieval. Below are the key aspects of the IndexManager:

### Index Metadata Structure

Metadata for indexes is stored in a file with the path `indexes/Table_Name/meta.data`. This metadata includes information such as the index name, column name, and root node offset of the B+ tree.

### Index Structure

Indexes are stored in files with paths like `indexes/Table_Name/Index_Name.data`. Each index follows the B+ tree structure for efficient data retrieval.

### Code of Conduct

The IndexManager follows specific guidelines to ensure consistency and integrity within the database. Key points include initializing indexes, updating them on row insertion or deletion, and handling operations like index or table deletion.

## Documentation

For detailed usage instructions and examples, please refer to the docs part in the repository. it provides comprehensive documentation on how to interact with both the HeapManager and IndexManager components.

Thank you for choosing our database storage engine! If you have any further questions or need assistance, feel free to reach out to our support team.
