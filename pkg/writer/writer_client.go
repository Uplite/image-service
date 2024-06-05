package writer

import (
	"context"

	"google.golang.org/grpc"

	"github.com/uplite/image-service/api/pb"
)

type writerClient struct {
	conn   *grpc.ClientConn
	client pb.ImageServiceWriterClient
}

func New(conn *grpc.ClientConn) *writerClient {
	return &writerClient{
		conn:   conn,
		client: pb.NewImageServiceWriterClient(conn),
	}
}

func (c *writerClient) Upload(ctx context.Context, opts ...grpc.CallOption) (pb.ImageServiceWriter_UploadClient, error) {
	return c.client.Upload(ctx, opts...)
}

func (c *writerClient) Delete(ctx context.Context, req *pb.DeleteRequest, opts ...grpc.CallOption) (*pb.DeleteResponse, error) {
	return c.client.Delete(ctx, req, opts...)
}

func (c *writerClient) Close() error {
	return c.conn.Close()
}
