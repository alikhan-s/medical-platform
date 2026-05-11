package repository

import (
	"context"
	"errors"
	"time"

	"github.com/alikhan-s/appointment-s/internal/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresAppointmentRepo struct {
	pool *pgxpool.Pool
}

func NewPostgresAppointmentRepo(pool *pgxpool.Pool) AppointmentRepository {
	return &postgresAppointmentRepo{pool: pool}
}

func (r *postgresAppointmentRepo) Create(ctx context.Context, appt *model.Appointment) error {
	appt.ID = uuid.New().String()
	now := time.Now().UTC()
	appt.CreatedAt = now
	appt.UpdatedAt = now

	_, err := r.pool.Exec(ctx,
		`INSERT INTO appointments (id, title, description, doctor_id, status, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		appt.ID, appt.Title, appt.Description, appt.DoctorID,
		string(appt.Status), appt.CreatedAt, appt.UpdatedAt,
	)
	return err
}

func (r *postgresAppointmentRepo) GetByID(ctx context.Context, id string) (*model.Appointment, error) {
	var a model.Appointment
	var status string

	err := r.pool.QueryRow(ctx,
		`SELECT id, title, description, doctor_id, status, created_at, updated_at
		 FROM appointments WHERE id = $1`,
		id,
	).Scan(&a.ID, &a.Title, &a.Description, &a.DoctorID, &status, &a.CreatedAt, &a.UpdatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, model.ErrAppointmentNotFound
	}
	if err != nil {
		return nil, err
	}
	a.Status = model.Status(status)
	return &a, nil
}

func (r *postgresAppointmentRepo) GetAll(ctx context.Context) ([]*model.Appointment, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, title, description, doctor_id, status, created_at, updated_at
		 FROM appointments ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var appts []*model.Appointment
	for rows.Next() {
		var a model.Appointment
		var status string
		if err := rows.Scan(&a.ID, &a.Title, &a.Description, &a.DoctorID,
			&status, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, err
		}
		a.Status = model.Status(status)
		appts = append(appts, &a)
	}
	return appts, rows.Err()
}

func (r *postgresAppointmentRepo) UpdateStatus(ctx context.Context, id string, status model.Status) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE appointments SET status = $1, updated_at = $2 WHERE id = $3`,
		string(status), time.Now().UTC(), id,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return model.ErrAppointmentNotFound
	}
	return nil
}
