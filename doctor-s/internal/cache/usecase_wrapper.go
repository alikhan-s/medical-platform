package cache

import (
	"context"
	"errors"
	"log"

	"github.com/alikhan-s/doctor-s/internal/model"
	"github.com/alikhan-s/doctor-s/internal/usecase"
)

const (
	entityDoctor     = "doctor"
	entityDoctorList = "list"
	listID           = "all"
)

type doctorCacheUseCase struct {
	inner usecase.DoctorUseCase
	cache CacheRepository
}

func NewDoctorCacheUseCase(inner usecase.DoctorUseCase, cache CacheRepository) usecase.DoctorUseCase {
	return &doctorCacheUseCase{inner: inner, cache: cache}
}

func (w *doctorCacheUseCase) CreateDoctor(ctx context.Context, doctor *model.Doctor) error {
	if err := w.inner.CreateDoctor(ctx, doctor); err != nil {
		return err
	}
	if err := w.cache.Delete(ctx, entityDoctorList, listID); err != nil {
		log.Printf("WARN [cache]: invalidate doctors:list after create: %v", err)
	}
	return nil
}

func (w *doctorCacheUseCase) GetDoctorByID(ctx context.Context, id string) (*model.Doctor, error) {
	var cached model.Doctor
	if err := w.cache.Get(ctx, entityDoctor, id, &cached); err == nil {
		return &cached, nil
	} else if !errors.Is(err, ErrCacheMiss) {
		log.Printf("WARN [cache]: get doctor %s: %v", id, err)
	}

	doctor, err := w.inner.GetDoctorByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if doctor != nil {
		if err := w.cache.Set(ctx, entityDoctor, id, doctor); err != nil {
			log.Printf("WARN [cache]: set doctor %s: %v", id, err)
		}
	}
	return doctor, nil
}

func (w *doctorCacheUseCase) GetAllDoctors(ctx context.Context) ([]*model.Doctor, error) {
	var cached []*model.Doctor
	if err := w.cache.Get(ctx, entityDoctorList, listID, &cached); err == nil {
		return cached, nil
	} else if !errors.Is(err, ErrCacheMiss) {
		log.Printf("WARN [cache]: get doctors:list: %v", err)
	}

	doctors, err := w.inner.GetAllDoctors(ctx)
	if err != nil {
		return nil, err
	}
	if err := w.cache.Set(ctx, entityDoctorList, listID, doctors); err != nil {
		log.Printf("WARN [cache]: set doctors:list: %v", err)
	}
	return doctors, nil
}
