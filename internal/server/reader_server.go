package server

import (
	"context"

	"google.golang.org/grpc"

	"github.com/uplite/image-service/api/pb"
	"github.com/uplite/image-service/internal/reader"
)

type readerServer struct {
	pb.UnimplementedImageServiceReaderServer
	reader reader.Reader
}

func newReaderServer(reader reader.Reader) *readerServer {
	return &readerServer{reader: reader}
}

func (s *readerServer) GetOne(ctx context.Context, req *pb.GetOneRequest) (*pb.GetOneResponse, error) {
	url, err := s.reader.ReadOne(ctx, req.GetKey())
	if err != nil {
		return nil, err
	}

	return &pb.GetOneResponse{Url: url}, nil
}

func (s *readerServer) GetMany(ctx context.Context, req *pb.GetManyRequest) (*pb.GetManyResponse, error) {
	urls, err := s.reader.ReadMany(ctx, req.GetUserPrefix())
	if err != nil {
		return nil, err
	}

	return &pb.GetManyResponse{Urls: urls}, nil
}

func (s *readerServer) registerServer(g *grpc.Server) {
	pb.RegisterImageServiceReaderServer(g, s)
}
