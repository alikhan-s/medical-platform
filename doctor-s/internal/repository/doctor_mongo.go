package repository

import (
	"context"

	"github.com/alikhan-s/doctor-s/internal/model"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type DoctorRepository interface {
	Create(ctx context.Context, doctor *model.Doctor) error
	GetByID(ctx context.Context, id string) (*model.Doctor, error)
	GetAll(ctx context.Context) ([]*model.Doctor, error)
	GetByEmail(ctx context.Context, email string) (*model.Doctor, error)
}

type doctorMongoRepo struct {
	db *mongo.Collection
}

func NewDoctorMongoRepo(db *mongo.Database) DoctorRepository {
	return &doctorMongoRepo{
		db: db.Collection("doctors"),
	}
}

func (r *doctorMongoRepo) Create(ctx context.Context, doctor *model.Doctor) error {
	doctor.ID = primitive.NewObjectID().Hex()
	_, err := r.db.InsertOne(ctx, doctor)
	return err
}

func (r *doctorMongoRepo) GetByID(ctx context.Context, id string) (*model.Doctor, error) {
	var doc model.Doctor
	err := r.db.FindOne(ctx, bson.M{"_id": id}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, model.ErrDoctorNotFound
		}
		return nil, err
	}
	return &doc, nil
}

func (r *doctorMongoRepo) GetAll(ctx context.Context) ([]*model.Doctor, error) {
	var doctors []*model.Doctor
	cursor, err := r.db.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, &doctors); err != nil {
		return nil, err
	}
	return doctors, nil
}

func (r *doctorMongoRepo) GetByEmail(ctx context.Context, email string) (*model.Doctor, error) {
	var doc model.Doctor
	err := r.db.FindOne(ctx, bson.M{"email": email}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return &doc, nil
}
