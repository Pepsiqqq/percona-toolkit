package utils

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
)

func runMinikube(args ...string) (string, error) {
	cmd := exec.Command("minikube", args...)

	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		return out.String(), fmt.Errorf("error running 'minikube %s': %v\nStderr: %s", args[0], err, stderr.String())
	}

	return out.String(), nil
}

func StartMinikube() {
	log.Println("Attempting to start Minikube...")
	out, err := runMinikube("start", "--memory=16000", "--cpus=16", "--disk-size=30g")
	if err != nil {
		log.Fatalf("Failed to start Minikube: %v", err)
	}
	log.Println(out)
}

func StopMinikube() {
	log.Println("Attempting to stop Minikube...")
	out, err := runMinikube("stop")
	if err != nil {
		log.Fatalf("Failed to stop Minikube: %v", err)
	}
	log.Println(out)
}

// getMinikubeStatus checks the status of the minikube cluster.
func GetMinikubeStatus() {
	log.Println("Checking Minikube status...")
	out, err := runMinikube("status")
	if err != nil {
		// "status" often returns a non-zero exit code if not running,
		// so we just print the error and output without failing.
		log.Printf("Minikube status check finished.\nOutput:\n%s\nError:\n%v", out, err)
	} else {
		log.Println("Minikube status check successful:")
		log.Println(out)
	}
}
