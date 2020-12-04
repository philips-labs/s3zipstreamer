package zip_streamer

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7"
)

type ZipStream struct {
	entries           []*FileEntry
	destination       io.Writer
	CompressionMethod uint16
}

func NewZipStream(entries []*FileEntry, w io.Writer) (*ZipStream, error) {
	if len(entries) == 0 {
		return nil, errors.New("must have at least 1 entry")
	}

	z := ZipStream{
		entries:     entries,
		destination: w,
		// Default to no compression to save CPU. Also ideal for streaming.
		CompressionMethod: zip.Store,
	}

	return &z, nil
}

func (z *ZipStream) StreamAllFiles(svc *minio.Client, bucket string) error {
	zipWriter := zip.NewWriter(z.destination)
	success := 0

	for _, entry := range z.entries {
		object, err := svc.GetObject(context.Background(), bucket, entry.s3Path, minio.GetObjectOptions{})
		if err != nil {
			fmt.Println(err)
			return err
		}

		header := &zip.FileHeader{
			Name:     entry.ZipPath(),
			Method:   z.CompressionMethod,
			Modified: time.Now(),
		}
		entryWriter, err := zipWriter.CreateHeader(header)
		if err != nil {
			object.Close()
			return err
		}

		// TODO: flush after every 32kb instead of every file to reduce memory
		_, err = io.Copy(entryWriter, object)
		if err != nil {
			object.Close()
			return err
		}

		zipWriter.Flush()
		flushingWriter, ok := z.destination.(http.Flusher)
		if ok {
			flushingWriter.Flush()
		}
		success++
	}

	if success == 0 {
		return errors.New("empty file - all files failed")
	}

	return zipWriter.Close()
}
