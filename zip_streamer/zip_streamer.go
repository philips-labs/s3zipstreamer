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

	fmt.Printf("Streaming all files...\n")
	buf := make([]byte, 4096)

	for _, entry := range z.entries {
		object, err := svc.GetObject(context.Background(), bucket, entry.s3Path, minio.GetObjectOptions{})
		if err != nil {
			fmt.Printf("error streaming %s: %v\n", entry.s3Path, err)
			return err
		}
		fmt.Printf("streaming: %s\n", entry.s3Path)
		header := &zip.FileHeader{
			Name:     entry.ZipPath(),
			Method:   z.CompressionMethod,
			Modified: time.Now(),
		}
		entryWriter, err := zipWriter.CreateHeader(header)
		if err != nil {
			object.Close()
			fmt.Printf("error while streaming %s: %v\n", entry.s3Path, err)
			return err
		}

		// TODO: flush after every 32kb instead of every file to reduce memory
		_, err = io.CopyBuffer(entryWriter, object, buf)
		if err != nil {
			object.Close()
			fmt.Printf("error copying %s: %v\n", entry.s3Path, err)
			return err
		}
		zipWriter.Flush()
		flushingWriter, ok := z.destination.(http.Flusher)
		if ok {
			flushingWriter.Flush()
		}
		object.Close()
		success++
	}

	if success == 0 {
		return errors.New("empty file - all files failed")
	}

	return zipWriter.Close()
}
