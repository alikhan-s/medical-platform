package event

import (
	"context"
	"log"
	"time"

	"github.com/alikhan-s/doctor-s/internal/model"
	"github.com/alikhan-s/doctor-s/internal/usecase"
)

type doctorCreatedEvent struct {
	EventType      string `json:"event_type"`
	OccurredAt     string `json:"occurred_at"`
	ID             string `json:"id"`
	FullName       string `json:"full_name"`
	Specialization string `json:"specialization"`
	Email          string `json:"email"`
}

type doctorEventUseCase struct {
	inner     usecase.DoctorUseCase
	publisher EventPublisher
}

func NewDoctorEventUseCase(inner usecase.DoctorUseCase, pub EventPublisher) usecase.DoctorUseCase {
	return &doctorEventUseCase{inner: inner, publisher: pub}
}

func (w *doctorEventUseCase) CreateDoctor(ctx context.Context, doctor *model.Doctor) error {
	if err := w.inner.CreateDoctor(ctx, doctor); err != nil {
		return err
	}

	evt := doctorCreatedEvent{
		EventType:      "doctors.created",
		OccurredAt:     time.Now().UTC().Format(time.RFC3339),
		ID:             doctor.ID,
		FullName:       doctor.FullName,
		Specialization: doctor.Specialization,
		Email:          doctor.Email,
	}
	if err := w.publisher.Publish(ctx, "doctors.created", evt); err != nil {
		log.Printf("ERROR [event]: publish doctors.created id=%s: %v", doctor.ID, err)
	}
	return nil
}

func (w *doctorEventUseCase) GetDoctorByID(ctx context.Context, id string) (*model.Doctor, error) {
	return w.inner.GetDoctorByID(ctx, id)
}

func (w *doctorEventUseCase) GetAllDoctors(ctx context.Context) ([]*model.Doctor, error) {
	return w.inner.GetAllDoctors(ctx)
}
