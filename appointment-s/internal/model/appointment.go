package model

import (
	"errors"
	"time"
)

type Status string

const (
	StatusNew        Status = "new"
	StatusInProgress Status = "in_progress"
	StatusDone       Status = "done"
)

var (
	ErrAppointmentNotFound = errors.New("appointment not found")
	ErrInvalidTitle        = errors.New("title is required")
	ErrInvalidDoctorID     = errors.New("doctor_id is required")
	ErrDoctorNotExists     = errors.New("the referenced doctor does not exist")
	ErrDoctorServiceDown   = errors.New("failed to communicate with Doctor Service")
	ErrInvalidStatus       = errors.New("invalid status value")
	ErrInvalidTransition   = errors.New("cannot transition status from done back to new")
)

type Appointment struct {
	ID          string    `json:"id" bson:"_id,omitempty"`
	Title       string    `json:"title" bson:"title"`
	Description string    `json:"description" bson:"description"`
	DoctorID    string    `json:"doctor_id" bson:"doctor_id"`
	Status      Status    `json:"status" bson:"status"`
	CreatedAt   time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" bson:"updated_at"`
}
