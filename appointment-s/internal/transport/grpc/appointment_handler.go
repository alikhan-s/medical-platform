package grpc

import (
	"context"

	"github.com/alikhan-s/appointment-s/internal/model"
	"github.com/alikhan-s/appointment-s/internal/usecase"
	pb "github.com/alikhan-s/appointment-s/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AppointmentHandler struct {
	pb.UnimplementedAppointmentServiceServer
	usecase usecase.AppointmentUseCase
}

func NewAppointmentHandler(u usecase.AppointmentUseCase) *AppointmentHandler {
	return &AppointmentHandler{usecase: u}
}

func (h *AppointmentHandler) CreateAppointment(ctx context.Context, req *pb.CreateAppointmentRequest) (*pb.AppointmentResponse, error) {
	appt := &model.Appointment{
		Title:       req.GetTitle(),
		Description: req.GetDescription(),
		DoctorID:    req.GetDoctorId(),
	}

	err := h.usecase.Create(ctx, appt)
	if err != nil {
		switch err {
		case model.ErrInvalidTitle, model.ErrInvalidDoctorID:
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case model.ErrDoctorNotExists:
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		case model.ErrDoctorServiceDown:
			return nil, status.Error(codes.Unavailable, err.Error())
		default:
			return nil, status.Error(codes.Internal, "Failed to create appointment")
		}
	}

	return mapAppointmentToProto(appt), nil
}

func (h *AppointmentHandler) GetAppointment(ctx context.Context, req *pb.GetAppointmentRequest) (*pb.AppointmentResponse, error) {
	appt, err := h.usecase.GetByID(ctx, req.GetId())
	if err != nil {
		if err == model.ErrAppointmentNotFound {
			return nil, status.Error(codes.NotFound, "appointment not found")
		}
		return nil, status.Error(codes.Internal, "Internal server error")
	}

	return mapAppointmentToProto(appt), nil
}

func (h *AppointmentHandler) ListAppointments(ctx context.Context, req *pb.ListAppointmentsRequest) (*pb.ListAppointmentsResponse, error) {
	appointments, err := h.usecase.GetAll(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to fetch appointments")
	}

	var pbAppts []*pb.AppointmentResponse
	for _, a := range appointments {
		pbAppts = append(pbAppts, mapAppointmentToProto(a))
	}

	return &pb.ListAppointmentsResponse{Appointments: pbAppts}, nil
}

func (h *AppointmentHandler) UpdateAppointmentStatus(ctx context.Context, req *pb.UpdateStatusRequest) (*pb.AppointmentResponse, error) {
	domainStatus := model.Status(req.GetStatus())

	err := h.usecase.UpdateStatus(ctx, req.GetId(), domainStatus)
	if err != nil {
		switch err {
		case model.ErrAppointmentNotFound:
			return nil, status.Error(codes.NotFound, err.Error())
		case model.ErrInvalidStatus, model.ErrInvalidTransition:
			return nil, status.Error(codes.InvalidArgument, err.Error())
		default:
			return nil, status.Error(codes.Internal, "Failed to update status")
		}
	}

	updatedAppt, _ := h.usecase.GetByID(ctx, req.GetId())
	return mapAppointmentToProto(updatedAppt), nil
}

func mapAppointmentToProto(a *model.Appointment) *pb.AppointmentResponse {
	if a == nil {
		return nil
	}
	return &pb.AppointmentResponse{
		Id:          a.ID,
		Title:       a.Title,
		Description: a.Description,
		DoctorId:    a.DoctorID,
		Status:      string(a.Status),
		CreatedAt:   a.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:   a.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
}
