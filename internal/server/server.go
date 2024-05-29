package server

import (
	"log"
	"net"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/grpc"

	"github.com/uplite/image-service/internal/config"
	"github.com/uplite/image-service/internal/reader"
	"github.com/uplite/image-service/internal/storage"
	"github.com/uplite/image-service/internal/writer"
)

type imageWriterServer struct {
	grpcServer   *grpc.Server
	writerServer *writerServer
}

type imageReaderServer struct {
	grpcServer   *grpc.Server
	readerServer *readerServer
}

func NewWriter() *imageWriterServer { return newImageWriterServer() }

func NewReader() *imageReaderServer { return newImageReaderServer() }

func newImageWriterServer() *imageWriterServer {
	client := s3.NewFromConfig(config.GetAwsConfig())

	grpcServer := grpc.NewServer()

	writerServer := newWriterServer(writer.NewStoreWriter(storage.NewS3Store(client, config.GetS3BucketName())))
	writerServer.registerServer(grpcServer)

	return &imageWriterServer{
		grpcServer:   grpcServer,
		writerServer: writerServer,
	}
}

func newImageReaderServer() *imageReaderServer {
	client := s3.NewFromConfig(config.GetAwsConfig())

	grpcServer := grpc.NewServer()

	readerServer := newReaderServer(reader.NewStoreReader(storage.NewS3Store(client, config.GetS3BucketName())))
	readerServer.registerServer(grpcServer)

	return &imageReaderServer{
		grpcServer:   grpcServer,
		readerServer: readerServer,
	}
}

func (s *imageWriterServer) Serve() error {
	lis, err := net.Listen("tcp", ":"+config.GetGrpcPort())
	if err != nil {
		log.Fatal(err)
	}

	return s.grpcServer.Serve(lis)
}

func (s *imageWriterServer) Close() {
	s.grpcServer.GracefulStop()
}

func (s *imageReaderServer) Serve() error {
	lis, err := net.Listen("tcp", ":"+config.GetGrpcPort())
	if err != nil {
		log.Fatal(err)
	}

	return s.grpcServer.Serve(lis)
}

func (s *imageReaderServer) Close() {
	s.grpcServer.GracefulStop()
}
