package dumper

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
)

func (d *Dumper) getIndividualFiles(job exportJob, crType string) {
	location := filepath.Join(d.location, job.Pod.Namespace, job.Pod.Name)
	for _, indf := range d.individualFiles {
		if indf.resourceName == crType {
			for _, path := range indf.filepaths {
				file, err := d.getFileFromPod(job.Pod, path, indf.containerName)
				if err != nil {
					d.logError(fmt.Errorf("error while getting individual files for \"%s\" pod and \"%s\" namespace to dump: %w, SKIPPING", job.Pod.Name, job.Pod.Namespace, err))
					continue
				}

				if len(file) != 0 {
					log.Printf("Writing individual file with path %s to dump", path)
					err = d.archive.WriteVirtualFile(location+"/"+path, file)
					if err != nil {
						d.logError(fmt.Errorf("error while writing individual files for \"%s\" pod and \"%s\" namespace to dump: %w", job.Pod.Name, job.Pod.Namespace, err))
					}
				}
			}
		}
	}
}

func (d *Dumper) getFileFromPod(pod corev1.Pod, filepath, containerName string) ([]byte, error) {
	if len(filepath) == 0 || len(containerName) == 0 {
		return nil, errors.New("container name or filepath is not specified")
	}

	cmd := []string{"tar", "cf", "-", filepath}
	stdout, stderr, err := d.executeInPod(nil, cmd, pod, containerName)
	if err != nil {
		return nil, fmt.Errorf("failed to execute command in Pod: stderr: %s: %w", &stderr, err)
	}

	tarReader := tar.NewReader(&stdout)
	var fileContentBuffer bytes.Buffer
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading tar header: %w", err)
		}

		if header.Typeflag == tar.TypeReg && header.Name == filepath {
			_, copyErr := io.Copy(&fileContentBuffer, tarReader)
			if copyErr != nil {
				return nil, fmt.Errorf("error copying file content: %w", copyErr)
			}
		}
	}

	return fileContentBuffer.Bytes(), nil
}
