package dumper

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"time"
)

// SpilloverBuffer holds data in RAM when exporting logs up to a limit, then switches to file on disk
type SpilloverBuffer struct {
	limit       int64
	currentSize int64
	memBuf      *bytes.Buffer
	file        *os.File
	filePath    string
	useDisk     bool
}

func NewSpilloverBuffer(limit int64) *SpilloverBuffer {
	return &SpilloverBuffer{
		limit:  limit,
		memBuf: new(bytes.Buffer),
	}
}

func (s *SpilloverBuffer) Write(p []byte) (n int, err error) {
	if s.useDisk {
		return s.file.Write(p)
	}

	if s.currentSize+int64(len(p)) > s.limit {
		if err := s.transitionToDisk(); err != nil {
			return 0, err
		}
		return s.file.Write(p)
	}
	s.currentSize += int64(len(p))
	return s.memBuf.Write(p)
}

func (s *SpilloverBuffer) transitionToDisk() error {
	f, err := os.CreateTemp("", "k8s-export-*.tmp")
	if err != nil {
		return err
	}
	s.filePath = f.Name()
	s.file = f
	s.useDisk = true
	if _, err := io.Copy(f, s.memBuf); err != nil {
		return err
	}
	s.memBuf.Reset()
	s.memBuf = nil
	return nil
}

func (s *SpilloverBuffer) Cleanup() {
	if s.file != nil {
		s.file.Close()
		os.Remove(s.filePath)
	}
}

func (s *SpilloverBuffer) WriteToTar(tw *tar.Writer, name string) error {
	var reader io.Reader
	var size int64
	if s.useDisk {
		s.file.Sync()
		info, _ := s.file.Stat()
		size = info.Size()
		s.file.Seek(0, 0)
		reader = s.file
	} else {
		size = int64(s.memBuf.Len())
		reader = s.memBuf
	}

	err := tw.WriteHeader(&tar.Header{
		Name:    name,
		Size:    size,
		Mode:    0644,
		ModTime: time.Now(),
	})

	if err != nil {
		return err
	}

	_, err = io.Copy(tw, reader)
	return err
}
