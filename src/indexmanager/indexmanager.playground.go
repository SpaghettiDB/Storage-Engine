package indexmanager

import (
	"fmt"
	"path"

	"github.com/krasun/fbptree"
)

func PlayGround() {

	dbPath := path.Join("./", "index.data")

	tree, err := fbptree.Open(dbPath, fbptree.PageSize(4096), fbptree.Order(500))
	if err != nil {
		panic(fmt.Errorf("failed to open B+ tree %s: %w", "./", err))
	}

	_, _, err = tree.Put([]byte("Hi!"), []byte("Hello world, B+ tree!"))
	if err != nil {
		panic(fmt.Errorf("failed to put: %w", err))
	}

	_, _, err = tree.Put([]byte("Does it override key?"), []byte("No!"))
	if err != nil {
		panic(fmt.Errorf("failed to put: %w", err))
	}

	_, _, err = tree.Put([]byte("Does it override key?"), []byte("Yes, absolutely! The key has been overridden."))
	if err != nil {
		panic(fmt.Errorf("failed to put: %w", err))
	}

	if err := tree.Close(); err != nil {
		panic(fmt.Errorf("failed to close: %w", err))
	}

	tree, err = fbptree.Open(dbPath, fbptree.PageSize(4096), fbptree.Order(500))
	if err != nil {
		panic(fmt.Errorf("failed to open B+ tree %s: %w", "./", err))
	}

	value, ok, err := tree.Get([]byte("Hi!"))
	if err != nil {
		panic(fmt.Errorf("failed to get value: %w", err))
	}
	if !ok {
		fmt.Println("failed to find value")
	}

	fmt.Println(string(value))

	value, ok, err = tree.Get([]byte("Does it override key?"))
	if err != nil {
		panic(fmt.Errorf("failed to get value: %w", err))
	}
	if !ok {
		fmt.Println("failed to find value")
	}

	if err := tree.Close(); err != nil {
		panic(fmt.Errorf("failed to close: %w", err))
	}

	fmt.Println(string(value))
	// Output:
	// Hello world, B+ tree!
	// Yes, absolutely! The key has been overridden.

}
