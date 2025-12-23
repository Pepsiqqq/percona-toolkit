package dumper

import (
	"context"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
)

func (d *Dumper) exportPodLogs(ctx context.Context, pod corev1.Pod, basePath string) error {
	containers := append(pod.Spec.InitContainers, pod.Spec.Containers...)

	if len(containers) == 0 {
		return fmt.Errorf("pod %s in namespace %s has no containers", pod.Name, pod.Namespace)
	}

	for _, container := range containers {
		buf := NewSpilloverBuffer(MaxRamPerLog)
		logOptions := &corev1.PodLogOptions{
			Container: container.Name,
		}

		req := d.clientSet.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, logOptions)
		stream, err := req.Stream(ctx)
		if err != nil {
			buf.Cleanup()
			return fmt.Errorf("failed to get logs for container %s, stopping export from whole pod: %w", container.Name, err)
		}
		_, err = io.Copy(buf, stream)
		stream.Close()
		if err != nil {
			buf.Cleanup()
			return fmt.Errorf("stream broken for %s: %w", container.Name, err)
		}

		containerLogPath := fmt.Sprintf("%s/%s.log", basePath, container.Name)
		d.archive.mu.Lock()
		writeErr := buf.WriteToTar(d.archive.tw, containerLogPath)
		d.archive.mu.Unlock()

		buf.Cleanup()
		if writeErr != nil {
			return writeErr
		}
	}

	return nil
}
