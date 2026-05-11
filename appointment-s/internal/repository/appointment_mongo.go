package repository

import (
	"context"
	"github.com/alikhan-s/appointment-s/internal/model"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type AppointmentRepository interface {
	Create(ctx context.Context, appt *model.Appointment) error
	GetByID(ctx context.Context, id string) (*model.Appointment, error)
	GetAll(ctx context.Context) ([]*model.Appointment, error)
	UpdateStatus(ctx context.Context, id string, status model.Status) error
}

type appointmentMongoRepo struct {
	db *mongo.Collection
}

func NewAppointmentMongoRepo(db *mongo.Database) AppointmentRepository {
	return &appointmentMongoRepo{
		db: db.Collection("appointments"),
	}
}

func (r *appointmentMongoRepo) Create(ctx context.Context, appt *model.Appointment) error {
	appt.ID = primitive.NewObjectID().Hex()
	_, err := r.db.InsertOne(ctx, appt)
	return err
}

func (r *appointmentMongoRepo) GetByID(ctx context.Context, id string) (*model.Appointment, error) {
	var appt model.Appointment
	err := r.db.FindOne(ctx, bson.M{"_id": id}).Decode(&appt)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, model.ErrAppointmentNotFound
		}
		return nil, err
	}
	return &appt, nil
}

func (r *appointmentMongoRepo) GetAll(ctx context.Context) ([]*model.Appointment, error) {
	var appointments []*model.Appointment
	cursor, err := r.db.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, &appointments); err != nil {
		return nil, err
	}
	return appointments, nil
}

func (r *appointmentMongoRepo) UpdateStatus(ctx context.Context, id string, status model.Status) error {
	update := bson.M{
		"$set": bson.M{
			"status":     status,
			"updated_at": time.Now(),
		},
	}
	result, err := r.db.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return err
	}
	if result.MatchedCount == 0 {
		return model.ErrAppointmentNotFound
	}
	return nil
}
