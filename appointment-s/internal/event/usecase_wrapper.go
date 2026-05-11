package event

import (
	"context"
	"log"
	"time"

	"github.com/alikhan-s/appointment-s/internal/model"
	"github.com/alikhan-s/appointment-s/internal/usecase"
)

type appointmentCreatedEvent struct {
	EventType  string `json:"event_type"`
	OccurredAt string `json:"occurred_at"`
	ID         string `json:"id"`
	Title      string `json:"title"`
	DoctorID   string `json:"doctor_id"`
	Status     string `json:"status"`
}

type appointmentStatusUpdatedEvent struct {
	EventType  string `json:"event_type"`
	OccurredAt string `json:"occurred_at"`
	ID         string `json:"id"`
	OldStatus  string `json:"old_status"`
	NewStatus  string `json:"new_status"`
}

type appointmentEventUseCase struct {
	inner     usecase.AppointmentUseCase
	publisher EventPublisher
}

func NewAppointmentEventUseCase(inner usecase.AppointmentUseCase, pub EventPublisher) usecase.AppointmentUseCase {
	return &appointmentEventUseCase{inner: inner, publisher: pub}
}

func (w *appointmentEventUseCase) Create(ctx context.Context, appt *model.Appointment) error {
	if err := w.inner.Create(ctx, appt); err != nil {
		return err
	}

	evt := appointmentCreatedEvent{
		EventType:  "appointments.created",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		ID:         appt.ID,
		Title:      appt.Title,
		DoctorID:   appt.DoctorID,
		Status:     string(appt.Status),
	}
	if err := w.publisher.Publish(ctx, "appointments.created", evt); err != nil {
		log.Printf("ERROR [event]: publish appointments.created id=%s: %v", appt.ID, err)
	}
	return nil
}

func (w *appointmentEventUseCase) GetByID(ctx context.Context, id string) (*model.Appointment, error) {
	return w.inner.GetByID(ctx, id)
}

func (w *appointmentEventUseCase) GetAll(ctx context.Context) ([]*model.Appointment, error) {
	return w.inner.GetAll(ctx)
}

func (w *appointmentEventUseCase) UpdateStatus(ctx context.Context, id string, newStatus model.Status) error {
	oldAppt, err := w.inner.GetByID(ctx, id)
	if err != nil {
		return err
	}
	oldStatus := string(oldAppt.Status)

	if err := w.inner.UpdateStatus(ctx, id, newStatus); err != nil {
		return err
	}

	evt := appointmentStatusUpdatedEvent{
		EventType:  "appointments.status_updated",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		ID:         id,
		OldStatus:  oldStatus,
		NewStatus:  string(newStatus),
	}
	if err := w.publisher.Publish(ctx, "appointments.status_updated", evt); err != nil {
		log.Printf("ERROR [event]: publish appointments.status_updated id=%s: %v", id, err)
	}
	return nil
}
