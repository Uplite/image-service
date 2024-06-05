package writer

import (
	"io"

	"github.com/uplite/image-service/api/pb"
)

type Client interface {
	pb.ImageServiceWriterClient
	io.Closer
}

var _ Client = (*writerClient)(nil)
