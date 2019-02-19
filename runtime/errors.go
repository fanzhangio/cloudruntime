package runtime

import (
	"errors"
	"fmt"
	"time"
)

// Common errors
var (
	ErrTaskNonRevertable = errors.New("task is not revertable")
)

// NotExistError indicates object doesn't exist
type NotExistError struct {
	ID string
}

// NotExist creates a NotExistError
func NotExist(id string) *NotExistError {
	return &NotExistError{ID: id}
}

// Error implements Error
func (e *NotExistError) Error() string {
	return "not exist: " + e.ID
}

// IsNotExist determines if an error is NotExistError
func IsNotExist(err error) bool {
	_, ok := err.(*NotExistError)
	return ok
}

// TaskErrorType indicates the error type
type TaskErrorType int

// Task error types
const (
	TaskErrIgnored TaskErrorType = iota // no error, same as success
	TaskErrFail
	TaskErrRetry
	TaskErrStuck
)

// TaskError is the type for error when task failed
type TaskError struct {
	TaskID     string        `json:"task-id"`     // task id
	Type       TaskErrorType `json:"type"`        // error type
	Message    string        `json:"message"`     // error Message
	Output     []byte        `json:"output"`      // arbitrary output
	Cause      error         `json:"cause"`       // cause of the error
	HappenedAt time.Time     `json:"happened-at"` // time when task failed
}

// NewTaskError constructs a TaskError
func NewTaskError(taskID string, errType TaskErrorType) *TaskError {
	return &TaskError{
		TaskID:     taskID,
		Type:       errType,
		HappenedAt: time.Now(),
	}
}

// SetMessage sets the message
func (e *TaskError) SetMessage(msg string) *TaskError {
	e.Message = msg
	return e
}

// SetOutput sets the output of the error
func (e *TaskError) SetOutput(output []byte) *TaskError {
	e.Output = output
	return e
}

// CausedBy sets the cause
func (e *TaskError) CausedBy(err error) *TaskError {
	e.Cause = err
	return e
}

// Error implements error
func (e *TaskError) Error() string {
	msg := fmt.Sprintf("Task[%s]: %d: %s @%s",
		e.TaskID, e.Type, e.Message, e.HappenedAt.Format(time.RFC3339))
	if e.Cause != nil {
		msg += "\nCaused by: " + e.Cause.Error()
	}
	if e.Output != nil {
		msg += "\nOutput:\n" + string(e.Output)
	}
	return msg
}
