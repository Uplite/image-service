package reader

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/uplite/image-service/api/pb"
	"github.com/uplite/image-service/internal/server"
)

type mockReader struct{}

func (r *mockReader) ReadOne(ctx context.Context, key string) (string, error) {
	return "image_url1", nil
}

func (r *mockReader) ReadMany(ctx context.Context, prefix string) ([]string, error) {
	return []string{"user1/image_url1", "user1/image_url2"}, nil
}

func TestSnowflakeClient(t *testing.T) {
	srv := server.NewReaderServer(new(mockReader))

	grpcServer := grpc.NewServer()

	pb.RegisterImageServiceReaderServer(grpcServer, srv)

	lis, err := net.Listen("tcp", ":50053")
	assert.NoError(t, err)

	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(":50053", grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)

	c := New(conn)

	t.Run("should get one image url", func(t *testing.T) {
		res, err := c.GetOne(context.Background(), &pb.GetOneRequest{Key: "image_url1"})
		assert.NoError(t, err)
		assert.Equal(t, "image_url1", res.GetUrl())
	})

	t.Run("should get many image urls", func(t *testing.T) {
		res, err := c.GetMany(context.Background(), &pb.GetManyRequest{UserPrefix: "user1"})
		assert.NoError(t, err)
		assert.Equal(t, []string{"user1/image_url1", "user1/image_url2"}, res.GetUrls())
	})
}
