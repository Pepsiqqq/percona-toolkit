package dumper

import (
	"archive/tar"
	"compress/gzip"
	"os"
	"sync"
	"time"
)

// tarWriter is a thread-safe wrapper around tar.Writer
type tarWriter struct {
	mu  sync.Mutex
	tw  *tar.Writer
	gz  *gzip.Writer
	out *os.File
}

func NewTarWriter(filename string) (*tarWriter, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	gz := gzip.NewWriter(f)

	return &tarWriter{
		tw:  tar.NewWriter(gz),
		gz:  gz,
		out: f,
	}, nil
}

func (s *tarWriter) Close() {
	s.tw.Close()
	s.gz.Close()
	s.out.Close()
}

func (s *tarWriter) WriteVirtualFile(path string, content []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	header := &tar.Header{
		Name:    path,
		Size:    int64(len(content)),
		Mode:    0644,
		ModTime: time.Now(),
	}

	if err := s.tw.WriteHeader(header); err != nil {
		return err
	}
	_, err := s.tw.Write(content)
	return err
}
