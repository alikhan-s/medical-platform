package cache

import (
	"context"
	"errors"
	"log"

	"github.com/alikhan-s/appointment-s/internal/model"
	"github.com/alikhan-s/appointment-s/internal/usecase"
)

const (
	entityAppointment     = "appointment"
	entityAppointmentList = "list"
	listID                = "all"
)

type appointmentCacheUseCase struct {
	inner usecase.AppointmentUseCase
	cache CacheRepository
}

func NewAppointmentCacheUseCase(inner usecase.AppointmentUseCase, cache CacheRepository) usecase.AppointmentUseCase {
	return &appointmentCacheUseCase{inner: inner, cache: cache}
}

func (w *appointmentCacheUseCase) Create(ctx context.Context, appt *model.Appointment) error {
	if err := w.inner.Create(ctx, appt); err != nil {
		return err
	}
	if err := w.cache.Delete(ctx, entityAppointmentList, listID); err != nil {
		log.Printf("WARN [cache]: invalidate appointments:list after create: %v", err)
	}
	return nil
}

func (w *appointmentCacheUseCase) GetByID(ctx context.Context, id string) (*model.Appointment, error) {
	var cached model.Appointment
	if err := w.cache.Get(ctx, entityAppointment, id, &cached); err == nil {
		return &cached, nil
	} else if !errors.Is(err, ErrCacheMiss) {
		log.Printf("WARN [cache]: get appointment %s: %v", id, err)
	}

	appt, err := w.inner.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if appt != nil {
		if err := w.cache.Set(ctx, entityAppointment, id, appt); err != nil {
			log.Printf("WARN [cache]: set appointment %s: %v", id, err)
		}
	}
	return appt, nil
}

func (w *appointmentCacheUseCase) GetAll(ctx context.Context) ([]*model.Appointment, error) {
	var cached []*model.Appointment
	if err := w.cache.Get(ctx, entityAppointmentList, listID, &cached); err == nil {
		return cached, nil
	} else if !errors.Is(err, ErrCacheMiss) {
		log.Printf("WARN [cache]: get appointments:list: %v", err)
	}

	appts, err := w.inner.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	if err := w.cache.Set(ctx, entityAppointmentList, listID, appts); err != nil {
		log.Printf("WARN [cache]: set appointments:list: %v", err)
	}
	return appts, nil
}

func (w *appointmentCacheUseCase) UpdateStatus(ctx context.Context, id string, newStatus model.Status) error {
	if err := w.inner.UpdateStatus(ctx, id, newStatus); err != nil {
		return err
	}

	updated, err := w.inner.GetByID(ctx, id)
	if err != nil {
		log.Printf("WARN [cache]: refetch appointment %s for write-through: %v", id, err)
		if delErr := w.cache.Delete(ctx, entityAppointment, id); delErr != nil {
			log.Printf("WARN [cache]: invalidate appointment %s: %v", id, delErr)
		}
	} else if updated != nil {
		if err := w.cache.Set(ctx, entityAppointment, id, updated); err != nil {
			log.Printf("WARN [cache]: set appointment %s: %v", id, err)
		}
	}

	if err := w.cache.Delete(ctx, entityAppointmentList, listID); err != nil {
		log.Printf("WARN [cache]: invalidate appointments:list after status update: %v", err)
	}
	return nil
}
