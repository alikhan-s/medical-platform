package client

import (
	"context"

	"github.com/alikhan-s/appointment-s/internal/model"
	doctorpb "github.com/alikhan-s/doctor-s/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DoctorClient interface {
	CheckDoctorExists(ctx context.Context, doctorID string) error
}

type doctorGRPCClient struct {
	client doctorpb.DoctorServiceClient
}

func NewDoctorGRPCClient(conn grpc.ClientConnInterface) DoctorClient {
	return &doctorGRPCClient{
		client: doctorpb.NewDoctorServiceClient(conn),
	}
}

func (c *doctorGRPCClient) CheckDoctorExists(ctx context.Context, doctorID string) error {
	req := &doctorpb.GetDoctorRequest{Id: doctorID}
	_, err := c.client.GetDoctor(ctx, req)

	if err != nil {
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.NotFound {
			return model.ErrDoctorNotExists
		}
		return model.ErrDoctorServiceDown
	}

	return nil
}
