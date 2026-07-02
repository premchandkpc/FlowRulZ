package common

import (
	"errors"
	"fmt"
)

type Class string

const (
	ClassPermanent Class = "permanent"
	ClassTransient Class = "transient"
	ClassAuth      Class = "auth"
	ClassNotFound  Class = "not_found"
	ClassConflict  Class = "conflict"
	ClassInternal  Class = "internal"
)

type Error struct {
	Class   Class
	Message string
	Code    string
	Cause   error
	Meta    map[string]any
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Class, e.Message, e.Cause)
	}
	return fmt.Sprintf("[%s] %s", e.Class, e.Message)
}

func (e *Error) Unwrap() error { return e.Cause }

func NewError(class Class, msg string) *Error {
	return &Error{Class: class, Message: msg}
}

func WrapError(class Class, msg string, cause error) *Error {
	return &Error{Class: class, Message: msg, Cause: cause}
}

func IsPermanent(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Class == ClassPermanent
	}
	return false
}

func IsTransient(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Class == ClassTransient
	}
	return false
}
