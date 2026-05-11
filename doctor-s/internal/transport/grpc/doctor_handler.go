package grpc

import (
	"context"

	"github.com/alikhan-s/doctor-s/internal/model"
	"github.com/alikhan-s/doctor-s/internal/usecase"
	pb "github.com/alikhan-s/doctor-s/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DoctorHandler struct {
	pb.UnimplementedDoctorServiceServer
	usecase usecase.DoctorUseCase
}

func NewDoctorHandler(u usecase.DoctorUseCase) *DoctorHandler {
	return &DoctorHandler{usecase: u}
}

func (h *DoctorHandler) CreateDoctor(ctx context.Context, req *pb.CreateDoctorRequest) (*pb.DoctorResponse, error) {
	doc := &model.Doctor{
		FullName:       req.GetFullName(),
		Specialization: req.GetSpecialization(),
		Email:          req.GetEmail(),
	}

	err := h.usecase.CreateDoctor(ctx, doc)
	if err != nil {
		switch err {
		case model.ErrInvalidFullName, model.ErrInvalidEmail, model.ErrInvalidEmailFormat:
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case model.ErrEmailExists:
			return nil, status.Error(codes.AlreadyExists, err.Error())
		default:
			return nil, status.Error(codes.Internal, "Failed to create doctor")
		}
	}

	return mapDoctorToProto(doc), nil
}

func (h *DoctorHandler) GetDoctor(ctx context.Context, req *pb.GetDoctorRequest) (*pb.DoctorResponse, error) {
	doc, err := h.usecase.GetDoctorByID(ctx, req.GetId())
	if err != nil {
		if err == model.ErrDoctorNotFound {
			return nil, status.Error(codes.NotFound, "there is no doctor like this")
		}
		return nil, status.Error(codes.Internal, "Internal server error")
	}

	return mapDoctorToProto(doc), nil
}

func (h *DoctorHandler) ListDoctors(ctx context.Context, req *pb.ListDoctorsRequest) (*pb.ListDoctorsResponse, error) {
	doctors, err := h.usecase.GetAllDoctors(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to fetch doctors")
	}

	var pbDoctors []*pb.DoctorResponse
	for _, d := range doctors {
		pbDoctors = append(pbDoctors, mapDoctorToProto(d))
	}

	return &pb.ListDoctorsResponse{Doctors: pbDoctors}, nil
}

func mapDoctorToProto(d *model.Doctor) *pb.DoctorResponse {
	return &pb.DoctorResponse{
		Id:             d.ID,
		FullName:       d.FullName,
		Specialization: d.Specialization,
		Email:          d.Email,
	}
}
