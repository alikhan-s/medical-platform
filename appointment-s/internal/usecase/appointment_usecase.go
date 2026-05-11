package usecase

import (
	"context"
	"github.com/alikhan-s/appointment-s/internal/client"
	"github.com/alikhan-s/appointment-s/internal/model"
	"github.com/alikhan-s/appointment-s/internal/repository"
	"time"
)

type AppointmentUseCase interface {
	Create(ctx context.Context, appt *model.Appointment) error
	GetByID(ctx context.Context, id string) (*model.Appointment, error)
	GetAll(ctx context.Context) ([]*model.Appointment, error)
	UpdateStatus(ctx context.Context, id string, newStatus model.Status) error
}

type appointmentUseCase struct {
	repo         repository.AppointmentRepository
	doctorClient client.DoctorClient
}

func NewAppointmentUseCase(repo repository.AppointmentRepository, doctorClient client.DoctorClient) AppointmentUseCase {
	return &appointmentUseCase{
		repo:         repo,
		doctorClient: doctorClient,
	}
}

func (u *appointmentUseCase) Create(ctx context.Context, appt *model.Appointment) error {
	if appt.Title == "" {
		return model.ErrInvalidTitle
	}
	if appt.DoctorID == "" {
		return model.ErrInvalidDoctorID
	}

	if err := u.doctorClient.CheckDoctorExists(ctx, appt.DoctorID); err != nil {
		return err
	}

	appt.Status = model.StatusNew
	appt.CreatedAt = time.Now()
	appt.UpdatedAt = time.Now()

	return u.repo.Create(ctx, appt)
}

func (u *appointmentUseCase) GetByID(ctx context.Context, id string) (*model.Appointment, error) {
	return u.repo.GetByID(ctx, id)
}

func (u *appointmentUseCase) GetAll(ctx context.Context) ([]*model.Appointment, error) {
	return u.repo.GetAll(ctx)
}

func (u *appointmentUseCase) UpdateStatus(ctx context.Context, id string, newStatus model.Status) error {

	if newStatus != model.StatusNew && newStatus != model.StatusInProgress && newStatus != model.StatusDone {
		return model.ErrInvalidStatus
	}

	currentAppt, err := u.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}

	if currentAppt.Status == model.StatusDone && newStatus == model.StatusNew {
		return model.ErrInvalidTransition
	}

	return u.repo.UpdateStatus(ctx, id, newStatus)
}
