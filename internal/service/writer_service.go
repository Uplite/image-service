package service

import (
	"log"
	"net"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/grpc"

	"github.com/uplite/image-service/api/pb"
	"github.com/uplite/image-service/internal/config"
	"github.com/uplite/image-service/internal/server"
	"github.com/uplite/image-service/internal/storage"
	"github.com/uplite/image-service/internal/writer"
)

type imageWriterService struct {
	grpcServer   *grpc.Server
	writerServer pb.ImageServiceWriterServer
}

func NewImageWriterService() *imageWriterService {
	c := s3.NewFromConfig(config.GetAwsConfig())
	g := grpc.NewServer()
	s := storage.NewS3Store(c, config.GetS3BucketName())
	w := writer.NewStoreWriter(s)

	writerServer := server.NewWriterServer(w)

	pb.RegisterImageServiceWriterServer(g, writerServer)

	return &imageWriterService{
		grpcServer:   g,
		writerServer: writerServer,
	}
}

func (s *imageWriterService) Serve() error {
	lis, err := net.Listen("tcp", ":"+config.GetGrpcPort())
	if err != nil {
		log.Fatal(err)
	}

	return s.grpcServer.Serve(lis)
}

func (s *imageWriterService) Close() {
	s.grpcServer.GracefulStop()
}
