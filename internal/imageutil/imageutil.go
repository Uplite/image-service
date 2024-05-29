package imageutil

import "github.com/uplite/image-service/api/pb"

type ContentType = string

const (
	ContentTypeJpeg ContentType = "image/jpeg"
	ContentTypePng  ContentType = "image/png"
	ContentTypeGif  ContentType = "image/gif"
	ContentTypeWebp ContentType = "image/webp"
	ContentTypeSvg  ContentType = "image/svg+xml"
	ContentTypeBmp  ContentType = "image/bmp"
)

func ContentTypeFrom(contentType pb.ImageContentType) ContentType {
	switch contentType {
	case pb.ImageContentType_IMAGE_CONTENT_TYPE_UNDEFINED:
		return ""
	case pb.ImageContentType_IMAGE_CONTENT_TYPE_JPEG:
		return ContentTypeJpeg
	case pb.ImageContentType_IMAGE_CONTENT_TYPE_PNG:
		return ContentTypePng
	case pb.ImageContentType_IMAGE_CONTENT_TYPE_GIF:
		return ContentTypeGif
	case pb.ImageContentType_IMAGE_CONTENT_TYPE_WEBP:
		return ContentTypeWebp
	case pb.ImageContentType_IMAGE_CONTENT_TYPE_SVG:
		return ContentTypeSvg
	case pb.ImageContentType_IMAGE_CONTENT_TYPE_BMP:
		return ContentTypeBmp
	default:
		return ""
	}
}
