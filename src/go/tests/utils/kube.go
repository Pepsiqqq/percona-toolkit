package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	pollInterval = 5 * time.Second
	pollTimeout  = 15 * time.Minute
)

func SetupK8sConcurrent(setupConfigs map[string]string) error {
	var wg sync.WaitGroup
	errChan := make(chan error, 1)
	StartMinikube()

	for operator, cluster := range setupConfigs {
		wg.Add(1)
		go func(operator, cluster string) {
			defer wg.Done()
			if err := deployToKube(operator); err != nil {
				errChan <- fmt.Errorf("error deploying %s: %w", operator, err)
			}
			if err := deployToKube(cluster); err != nil {
				errChan <- fmt.Errorf("error deploying %s: %w", cluster, err)
			}
		}(operator, cluster)
	}
	go func() {
		wg.Wait()
		close(errChan)
	}()
	if err, ok := <-errChan; ok {
		return err
	}
	return nil
}

func deployToKube(yamlPath string) error {
	client, mapper, err := getDynamicClientAndRestMapper()
	if err != nil {
		return fmt.Errorf("failed to create kube client and rest mapper: %w", err)
	}

	gvrs, err := applyYAMLtoClient(context.Background(), client, mapper, yamlPath)
	if err != nil {
		return fmt.Errorf("failed to apply operator YAML: %w", err)
	}

	mapper.Reset()

	fmt.Printf("Waiting for pod(s) to be ready...\n")
	err = waitForReady(client, gvrs)
	if err != nil {
		return fmt.Errorf("pods failed to become ready: %w", err)
	}

	return nil
}

func getDynamicClientAndRestMapper() (*dynamic.DynamicClient, *restmapper.DeferredDiscoveryRESTMapper, error) {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get user home directory: %w", err)
		}
		kubeconfig = filepath.Join(home, ".kube", "config")
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build config from flags: %w", err)
	}
	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}
	dc, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))

	return dynClient, mapper, nil
}

func GetKubeConfigString() (string, error) {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to get user home directory: %w", err)
		}
		kubeconfig = filepath.Join(home, ".kube", "config")
	}
	return kubeconfig, nil
}

func applyYAMLtoClient(ctx context.Context, client dynamic.Interface, mapper meta.RESTMapper, file_path string) ([]schema.GroupVersionResource, error) {
	var gvrs []schema.GroupVersionResource
	content, err := os.ReadFile(file_path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", file_path, err)
	}
	decoder := yaml.NewYAMLOrJSONDecoder(strings.NewReader(string(content)), 10000)
	for {
		var obj *unstructured.Unstructured
		err := decoder.Decode(&obj)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to decode YAML document: %w", err)
		}

		if obj == nil {
			continue
		}

		if len(obj.Object) == 0 {
			continue
		}

		mapping, err := mapper.RESTMapping(obj.GroupVersionKind().GroupKind(), obj.GroupVersionKind().Version)
		if err != nil {
			return nil, fmt.Errorf("failed to get REST mapping for %s: %w", obj.GroupVersionKind().String(), err)
		}
		gvrs = append(gvrs, mapping.Resource)

		var dr dynamic.ResourceInterface
		if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
			if obj.GetNamespace() == "" {
				obj.SetNamespace("default")
			}
			dr = client.Resource(mapping.Resource).Namespace(obj.GetNamespace())
		} else {
			dr = client.Resource(mapping.Resource)
		}

		data, err := json.Marshal(obj.Object)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal object to JSON: %w", err)
		}

		_, err = dr.Patch(ctx, obj.GetName(), types.ApplyPatchType, data, metav1.PatchOptions{
			FieldManager: "go-percona-toolkit",
		})

		if err != nil {
			return nil, fmt.Errorf("failed to apply resource %s/%s: %w", obj.GroupVersionKind().Kind, obj.GetName(), err)
		}

		fmt.Printf("Applied %s/%s\n", obj.GroupVersionKind().Kind, obj.GetName())
	}
	return gvrs, nil
}

func waitForReady(dynamicClient dynamic.Interface, gvrs []schema.GroupVersionResource) error {
	var condition wait.ConditionWithContextFunc
	for _, gvr := range gvrs {
		if gvr.Resource == "deployments" {
			condition = getConditionDeployments(dynamicClient, gvr)
		} else {
			condition = getConditionCluster(dynamicClient, gvr)
		}
	}

	fmt.Printf("Waiting for pods to be ready for up to %s...\n", pollTimeout)
	err := wait.PollUntilContextTimeout(context.Background(), pollInterval, pollTimeout, true, condition)
	if err != nil {
		if wait.Interrupted(err) && context.Background().Err() == context.DeadlineExceeded {
			return fmt.Errorf("timeout exceeded (%s) waiting for pod to be ready: %w", pollTimeout, err)
		}
		return fmt.Errorf("error while waiting for pod: %w", err)
	}

	return nil
}
func getConditionDeployments(dynamicClient dynamic.Interface, gvr schema.GroupVersionResource) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		unstructuredList, err := dynamicClient.Resource(gvr).List(ctx, metav1.ListOptions{})
		if err != nil {
			log.Printf("Error listing pods with dynamic client: %v", err)
			return false, nil
		}

		for _, unstr := range unstructuredList.Items {
			specReplicas, found, err := unstructured.NestedInt64(unstr.Object, "spec", "replicas")
			if err != nil {
				return false, fmt.Errorf("error when getting spec replicas from deployment: %w", err)
			}
			if !found {
				return false, errors.New("no specReplicas was found")
			}
			if specReplicas == 0 {
				return false, nil
			}
			statusReadyReplicas, found, err := unstructured.NestedInt64(unstr.Object, "status", "readyReplicas")
			if err != nil {
				return false, fmt.Errorf("error reading status.readyReplicas: %w", err)
			}
			if !found {
				statusReadyReplicas = 0
			}

			if specReplicas > 0 && specReplicas != statusReadyReplicas {
				return false, nil
			}
		}
		return true, nil
	}
}

func getConditionCluster(dynamicClient dynamic.Interface, gvr schema.GroupVersionResource) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		unstructuredList, err := dynamicClient.Resource(gvr).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, fmt.Errorf("error listing pods with dynamic client: %w", err)
		}
		for _, unstr := range unstructuredList.Items {
			statusValue, found, err := unstructured.NestedString(unstr.Object, "status", "state")
			if err != nil {
				return false, fmt.Errorf("error when getting nested string status: %w", err)
			}
			if !found {
				return false, nil
			}
			// if statusValue == "error" {
			// 	return false, errors.New("status of cluster is error: " + gvr.Resource)
			// }
			if statusValue != "ready" {
				return false, nil
			}
		}
		fmt.Printf("%v is ready\n", gvr.Resource)
		return true, nil
	}
}
