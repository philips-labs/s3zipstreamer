package zip_streamer

import (
	"errors"
	"path"
)

type FileEntry struct {
	s3Path  string
	zipPath string
}

func NewFileEntry(s3Path string, zipPath string) (*FileEntry, error) {
	zipPath = path.Clean(zipPath)
	if path.IsAbs(zipPath) {
		return nil, errors.New("zip path must be relative")
	}
	if filename := path.Base(zipPath); len(filename) == 0 || filename == "." {
		return nil, errors.New("zip path must have file")
	}

	f := FileEntry{
		s3Path:  s3Path,
		zipPath: zipPath,
	}
	return &f, nil
}

func (f *FileEntry) S3Path() string {
	return f.s3Path
}

func (f *FileEntry) ZipPath() string {
	return f.zipPath
}
