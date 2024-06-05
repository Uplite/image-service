package reader

import (
	"io"

	"github.com/uplite/image-service/api/pb"
)

type Client interface {
	pb.ImageServiceReaderClient
	io.Closer
}

var _ Client = (*readerClient)(nil)
