package server

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/uplite/image-service/api/pb"
	"github.com/uplite/image-service/internal/imageutil"
	"github.com/uplite/image-service/internal/writer"
)

const (
	ErrNoContentType = "content_type cannot be empty"
	ErrNoKey         = "key cannot be empty"
)

type writerServer struct {
	pb.UnimplementedImageServiceWriterServer

	writer writer.WriterDeleter
}

func NewWriterServer(writer writer.WriterDeleter) *writerServer {
	return &writerServer{writer: writer}
}

func newUploadError() *pb.UploadResponse {
	return &pb.UploadResponse{UploadStatus: pb.UploadStatus_UPLOAD_STATUS_ERROR}
}

func newUploadSuccess() *pb.UploadResponse {
	return &pb.UploadResponse{UploadStatus: pb.UploadStatus_UPLOAD_STATUS_SUCCESS}
}

func (s *writerServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := s.writer.Delete(ctx, req.GetKey()); err != nil {
		return nil, err
	}

	return &pb.DeleteResponse{Ok: true}, nil
}

func (s *writerServer) Upload(stream pb.ImageServiceWriter_UploadServer) error {
	ctx := stream.Context()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	var buf bytes.Buffer
	var imageKey string
	var contentType string

	for {
		msg, recvErr := stream.Recv()
		if recvErr != nil {
			if recvErr == io.EOF {
				break
			}
			return recvErr
		}

		if imageKey == "" {
			imageKey = msg.GetKey()
		}

		if contentType == "" {
			contentType = imageutil.ContentTypeFrom(msg.GetContentType())
		}

		buf.Write(msg.GetData())
	}

	if imageKey == "" {
		return errors.New(ErrNoKey)
	}

	if contentType == "" {
		return errors.New(ErrNoContentType)
	}

	if err := s.writer.Write(ctx, imageKey, contentType, &buf); err != nil {
		return stream.SendAndClose(newUploadError())
	}

	return stream.SendAndClose(newUploadSuccess())
}
