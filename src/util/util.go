package util

import (
	"fmt"
)

func GetFirstElement[T any](collection []T) (T, error) {
	if len(collection) == 0 {
		var zero T
		return zero, fmt.Errorf("the collection is empty")
	}
	return collection[0], nil
}
