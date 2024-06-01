package service

import (
	"log"
	"net"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/grpc"

	"github.com/uplite/image-service/api/pb"
	"github.com/uplite/image-service/internal/config"
	"github.com/uplite/image-service/internal/reader"
	"github.com/uplite/image-service/internal/server"
	"github.com/uplite/image-service/internal/storage"
)

type imageReaderService struct {
	grpcServer   *grpc.Server
	readerServer pb.ImageServiceReaderServer
}

func NewImageReaderService() *imageReaderService {
	c := s3.NewFromConfig(config.GetAwsConfig())
	g := grpc.NewServer()
	s := storage.NewS3Store(c, config.GetS3BucketName())
	r := reader.NewStoreReader(s)

	readerServer := server.NewReaderServer(r)

	pb.RegisterImageServiceReaderServer(g, readerServer)

	return &imageReaderService{
		grpcServer:   g,
		readerServer: readerServer,
	}
}

func (s *imageReaderService) Serve() error {
	lis, err := net.Listen("tcp", ":"+config.GetGrpcPort())
	if err != nil {
		log.Fatal(err)
	}

	return s.grpcServer.Serve(lis)
}

func (s *imageReaderService) Close() {
	s.grpcServer.GracefulStop()
}
