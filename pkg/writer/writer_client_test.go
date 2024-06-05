package writer

import (
	"bytes"
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/uplite/image-service/api/pb"
	"github.com/uplite/image-service/internal/server"
)

type mockWriter struct {
	db map[string]*bytes.Buffer
}

func (w *mockWriter) Write(ctx context.Context, key, contentType string, buf *bytes.Buffer) error {
	if w.db == nil {
		w.db = make(map[string]*bytes.Buffer)
	}
	w.db[key] = buf
	return nil
}

func (w *mockWriter) Delete(ctx context.Context, key string) error {
	w.db[key] = new(bytes.Buffer)
	return nil
}

func TestWriterClient(t *testing.T) {
	w := new(mockWriter)

	srv := server.NewWriterServer(w)

	grpcServer := grpc.NewServer()

	pb.RegisterImageServiceWriterServer(grpcServer, srv)

	lis, err := net.Listen("tcp", ":50054")
	assert.NoError(t, err)

	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(":50054", grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)

	c := New(conn)

	t.Run("should upload", func(t *testing.T) {
		stream, err := c.Upload(context.Background())
		defer stream.CloseSend()
		assert.NoError(t, err)

		chunkOne := &pb.UploadRequest{Key: "key", Data: []byte{0, 1, 2}, ContentType: pb.ImageContentType_IMAGE_CONTENT_TYPE_JPEG}
		chunkTwo := &pb.UploadRequest{Key: "key", Data: []byte{3, 4, 5}, ContentType: pb.ImageContentType_IMAGE_CONTENT_TYPE_JPEG}

		err = stream.Send(chunkOne)
		assert.NoError(t, err)

		err = stream.Send(chunkTwo)
		assert.NoError(t, err)

		res, err := stream.CloseAndRecv()
		assert.NoError(t, err)
		assert.Equal(t, pb.UploadStatus_UPLOAD_STATUS_SUCCESS, res.GetUploadStatus())

		assert.Equal(t, bytes.NewBuffer([]byte{0, 1, 2, 3, 4, 5}), w.db["key"])
	})

	t.Run("should delete", func(t *testing.T) {
		res, err := c.Delete(context.Background(), &pb.DeleteRequest{Key: "key"})
		assert.NoError(t, err)
		assert.Equal(t, true, res.GetOk())
		assert.Equal(t, new(bytes.Buffer), w.db["key"])
	})
}
