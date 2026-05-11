package usecase

import (
	"context"
	"regexp"

	"github.com/alikhan-s/doctor-s/internal/model"
	"github.com/alikhan-s/doctor-s/internal/repository"
)

type DoctorUseCase interface {
	CreateDoctor(ctx context.Context, doctor *model.Doctor) error
	GetDoctorByID(ctx context.Context, id string) (*model.Doctor, error)
	GetAllDoctors(ctx context.Context) ([]*model.Doctor, error)
}

type doctorUseCase struct {
	repo repository.DoctorRepository
}

func NewDoctorUseCase(repo repository.DoctorRepository) DoctorUseCase {
	return &doctorUseCase{repo: repo}
}

func (u *doctorUseCase) CreateDoctor(ctx context.Context, doctor *model.Doctor) error {
	if doctor.FullName == "" {
		return model.ErrInvalidFullName
	}
	if doctor.Email == "" {
		return model.ErrInvalidEmail
	}

	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	if !emailRegex.MatchString(doctor.Email) {
		return model.ErrInvalidEmailFormat
	}

	existingDoc, err := u.repo.GetByEmail(ctx, doctor.Email)
	if err != nil {
		return err
	}
	if existingDoc != nil {
		return model.ErrEmailExists
	}

	return u.repo.Create(ctx, doctor)
}

func (u *doctorUseCase) GetDoctorByID(ctx context.Context, id string) (*model.Doctor, error) {
	return u.repo.GetByID(ctx, id)
}

func (u *doctorUseCase) GetAllDoctors(ctx context.Context) ([]*model.Doctor, error) {
	return u.repo.GetAll(ctx)
}
