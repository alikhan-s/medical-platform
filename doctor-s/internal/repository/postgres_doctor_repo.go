package repository

import (
	"context"
	"errors"

	"github.com/alikhan-s/doctor-s/internal/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresDoctorRepo struct {
	pool *pgxpool.Pool
}

// NewPostgresDoctorRepo creates a PostgreSQL-backed DoctorRepository.
func NewPostgresDoctorRepo(pool *pgxpool.Pool) DoctorRepository {
	return &postgresDoctorRepo{pool: pool}
}

func (r *postgresDoctorRepo) Create(ctx context.Context, doctor *model.Doctor) error {
	doctor.ID = uuid.New().String()

	_, err := r.pool.Exec(ctx,
		`INSERT INTO doctors (id, full_name, specialization, email)
		 VALUES ($1, $2, $3, $4)`,
		doctor.ID, doctor.FullName, doctor.Specialization, doctor.Email,
	)
	if isUniqueViolation(err) {
		return model.ErrEmailExists
	}
	return err
}

func (r *postgresDoctorRepo) GetByID(ctx context.Context, id string) (*model.Doctor, error) {
	var d model.Doctor
	err := r.pool.QueryRow(ctx,
		`SELECT id, full_name, specialization, email FROM doctors WHERE id = $1`,
		id,
	).Scan(&d.ID, &d.FullName, &d.Specialization, &d.Email)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, model.ErrDoctorNotFound
	}
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (r *postgresDoctorRepo) GetAll(ctx context.Context) ([]*model.Doctor, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, full_name, specialization, email FROM doctors ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var doctors []*model.Doctor
	for rows.Next() {
		var d model.Doctor
		if err := rows.Scan(&d.ID, &d.FullName, &d.Specialization, &d.Email); err != nil {
			return nil, err
		}
		doctors = append(doctors, &d)
	}
	return doctors, rows.Err()
}

func (r *postgresDoctorRepo) GetByEmail(ctx context.Context, email string) (*model.Doctor, error) {
	var d model.Doctor
	err := r.pool.QueryRow(ctx,
		`SELECT id, full_name, specialization, email FROM doctors WHERE email = $1`,
		email,
	).Scan(&d.ID, &d.FullName, &d.Specialization, &d.Email)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // caller treats nil,nil as "not found"
	}
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
