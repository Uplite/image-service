package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/planetscale/vtprotobuf/codec/grpc"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"

	"github.com/uplite/image-service/internal/service"
)

func init() {
	encoding.RegisterCodec(grpc.Codec{})
}

func main() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	imageWriter := service.NewImageWriterService()

	go startService(imageWriter)

	<-stop

	stopService(imageWriter)
}

func startService(s service.Service) {
	if err := s.Serve(); err != nil {
		log.Fatal(err)
	}
}

func stopService(s service.Service) {
	s.Close()
}
