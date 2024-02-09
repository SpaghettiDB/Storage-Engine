
package errors

import "fmt"


type ResourceType string

const (
	Heap ResourceType = "Table"
	// you can add more resource types here
)

type ResourceAlreadyExistsError struct {
	ResourceType ResourceType
	ResourceName string
}

// Error returns the error message.
func (e *ResourceAlreadyExistsError) Error() string {
	return fmt.Sprintf("%s with name %s already exists", e.ResourceType, e.ResourceName)
}
