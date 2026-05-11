package model

import "errors"

var (
	ErrDoctorNotFound     = errors.New("doctor not found")
	ErrEmailExists        = errors.New("doctor with this email already exists")
	ErrInvalidFullName    = errors.New("full_name is required")
	ErrInvalidEmail       = errors.New("email is required")
	ErrInvalidEmailFormat = errors.New("invalid email format (missing @ or domain)")
)

type Doctor struct {
	ID             string `json:"id" bson:"_id,omitempty"`
	FullName       string `json:"full_name" bson:"full_name"`
	Specialization string `json:"specialization" bson:"specialization"`
	Email          string `json:"email" bson:"email"`
}
