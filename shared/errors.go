package shared

import (
	"fmt"
	"sync"
)

var (
	oneErrorFactory sync.Once
	ErrorCentral    ErrorFactory
)

func init() {
	oneErrorFactory.Do(func() {
		ErrorCentral = &errorFactory{}
	})
}

type ErrorFactory interface {
	InvalidPath(path, detail string) error
	InvalidFilter(filter, detail string) error
	InvalidType(path, expect, got string) error
	NoAttribute(path string) error
	Text(template string, args ...interface{}) error
}

type errorFactory struct{}

func (f *errorFactory) Text(template string, args ...interface{}) error {
	return fmt.Errorf(template, args...)
}

func (f *errorFactory) InvalidPath(path, detail string) error {
	return &InvalidPathError{path, detail}
}

// Invalid Path
type InvalidPathError struct {
	Path   string
	Detail string
}

func (e InvalidPathError) Error() string {
	return fmt.Sprintf("Path [%s] is invalid: %s", e.Path, e.Detail)
}

func (f *errorFactory) InvalidFilter(filter, detail string) error {
	return &InvalidFilterError{filter, detail}
}

// Invalid Filter
type InvalidFilterError struct {
	Filter string
	Detail string
}

func (e InvalidFilterError) Error() string {
	return fmt.Sprintf("Filter [%s] is invalid: %s", e.Filter, e.Detail)
}

func (f *errorFactory) InvalidType(path, expect, got string) error {
	return &InvalidTypeError{path, expect, got}
}

// Invalid Type
type InvalidTypeError struct {
	Path   string
	Expect string
	Got    string
}

func (e *InvalidTypeError) Error() string {
	return fmt.Sprintf("Invalid type at '%s', expected '%s', got '%s'", e.Path, e.Expect, e.Got)
}

func (f *errorFactory) NoAttribute(path string) error {
	return &NoAttributeError{path}
}

// No Attribute
type NoAttributeError struct {
	Path string
}

func (e *NoAttributeError) Error() string {
	return fmt.Sprintf("No attribute defined for path (segment) '%s'", e.Path)
}