package dumper

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/transport/spdy"
	yamlutil "sigs.k8s.io/yaml"
)

// Get all pods as list from namespace
func (d *Dumper) getPodList(namespace string) (*corev1.PodList, error) {
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	unstructuredPods, err := d.dynamicClient.Resource(gvr).Namespace(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("error listing pods in namespace %s: %w", namespace, err)
	}
	podList := &corev1.PodList{}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredPods.UnstructuredContent(), podList)
	if err != nil {
		return nil, fmt.Errorf("error converting UnstructuredList to PodList while getting pods from \"%s\" namespace: %w", namespace, err)
	}
	return podList, nil
}

// Get all namespaces as list
func (d *Dumper) getNamespacesList() (*corev1.NamespaceList, error) {
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}
	unstructuredNamespaces, err := d.dynamicClient.Resource(gvr).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("error listing namespaces: %w", err)
	}
	namespacesList := &corev1.NamespaceList{}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredNamespaces.UnstructuredContent(), namespacesList)
	if err != nil {
		return nil, fmt.Errorf("error converting UnstructuredList to NamespaceList: %w", err)
	}
	return namespacesList, nil
}

// Gets list of unstructured objects from provided gvr and namespace and returns it
func (d *Dumper) getUnstructuredListWithNamespace(gvr schema.GroupVersionResource, namespace string) (*unstructured.UnstructuredList, error) {
	list, err := d.dynamicClient.Resource(gvr).Namespace(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get list from provided gvr %v: %w", gvr, err)
	}
	return list, nil
}

// Get all nodes as list
func (d *Dumper) getNodeList() (*corev1.NodeList, error) {
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}
	unstructuredNodes, err := d.dynamicClient.Resource(gvr).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("error listing nodes in namespace: %w", err)
	}
	nodeList := &corev1.NodeList{}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredNodes.UnstructuredContent(), nodeList)
	if err != nil {
		return nil, fmt.Errorf("error converting UnstructuredList to NodeList: %w", err)
	}
	return nodeList, nil
}

// Gets list of unstructured objects from provided gvr and returns it
func (d *Dumper) getUnstructuredList(gvr schema.GroupVersionResource) (*unstructured.UnstructuredList, error) {
	list, err := d.dynamicClient.Resource(gvr).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get list from provided gvr %v: %w", gvr, err)
	}
	return list, nil
}

// Converts provided gvr into yamlBytes. Works close to kubectl command: "kubectl get `name` -o yaml"
func (d *Dumper) getResource(gvr schema.GroupVersionResource) ([]byte, error) {
	list, err := d.getUnstructuredList(gvr)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("resource type %s in namespace was not found: %w", gvr.Resource, err)
		}
		return nil, fmt.Errorf("failed to list resources of type in namespace %s: %w", gvr.Resource, err)
	}
	runtimeObj := runtime.Object(list)
	jsonBytes, err := runtime.Encode(unstructured.UnstructuredJSONScheme, runtimeObj)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal list to JSON: %w", err)
	}

	yamlBytes, err := yamlutil.JSONToYAML(jsonBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to convert JSON to YAML: %w", err)
	}

	return yamlBytes, nil
}

/*
Executes a command in the pod and returns the standard output and error streams.
The argument 'stdin' is used for piping and can be set to nil if not required.

an example of command is: command := []string{"psql", "-X", "-f", "-"}

The container can be an empty string, but the following rules will be applied:

1. If the pod has only one container, the command will be executed in that container.

2. If the pod has multiple containers, it will attempt to use the
container specified by the 'kubectl.kubernetes.io/default-container' annotation on the pod, if present.

3. If no annotation is present and there are multiple containers, this will return an error.
*/
func (d *Dumper) executeInPod(stdin io.Reader, command []string, pod corev1.Pod, container string) (bytes.Buffer, bytes.Buffer, error) {
	stdinFlag := false
	if stdin != nil {
		stdinFlag = true
	}
	var outb, errb bytes.Buffer
	req := d.clientSet.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Command:   command,
			Stdin:     stdinFlag,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
			Container: container,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(d.restConfig, "POST", req.URL())
	if err != nil {
		return outb, errb, fmt.Errorf("error creating SPDY executor: %w", err)
	}

	err = exec.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: &outb,
		Stderr: &errb,
		Tty:    false,
	})
	if err != nil {
		return outb, errb, fmt.Errorf("error executing remote command: %w", err)
	}

	return outb, errb, nil
}

// Searches for a secret in the pod with the secretName attribute and returns it. An example of a name is "MONGODB_DATABASE_ADMIN_USER".
func (d *Dumper) getSecretValueFromPod(pod corev1.Pod, secretName string) (string, error) {
	for _, volume := range pod.Spec.Volumes {
		if volume.Secret == nil {
			continue
		}

		secret, err := d.clientSet.CoreV1().Secrets(pod.Namespace).Get(context.TODO(), volume.Secret.SecretName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return "", fmt.Errorf("error fetching secret '%s/%s': %w", pod.Namespace, volume.Secret.SecretName, err)
		}

		secretValueBytes, found := secret.Data[secretName]
		if found {
			return string(secretValueBytes), nil
		}
	}
	return "", fmt.Errorf("could not find any secret with name %s in Pod '%s/%s", secretName, pod.Namespace, pod.Name)
}

/*
Forwards ports to a specific pod. Close the returned channel to stop forwarding.
It accepts multiple port bindings in the format "12345:12345".
*/
func (d *Dumper) portForwardPod(pod corev1.Pod, ports []string) (chan struct{}, error) {
	apiURL, err := url.Parse(d.restConfig.Host)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config host URL: %w", err)
	}
	path := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s/portforward", apiURL.Path, pod.Namespace, pod.Name)
	hostURL := url.URL{
		Scheme: apiURL.Scheme,
		Host:   apiURL.Host,
		Path:   path,
	}

	roundTripper, upgrader, err := spdy.RoundTripperFor(d.restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create roundtripper and upgrader: %w", err)
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: roundTripper}, http.MethodPost, &hostURL)
	stopChan := make(chan struct{}, 1)
	readyChan := make(chan struct{}, 1)
	out, errOut := new(bytes.Buffer), new(bytes.Buffer)
	forwarder, err := portforward.New(dialer, ports, stopChan, readyChan, out, errOut)
	if err != nil {
		return nil, fmt.Errorf("failed to create port forwarder: %w", err)
	}

	go func() {
		if err = forwarder.ForwardPorts(); err != nil {
			fmt.Fprintf(errOut, "Port forwarding failed: %v\n", err)
			close(stopChan)
		}
	}()

	select {
	case <-readyChan:
		return stopChan, nil
	case <-stopChan:
		return nil, fmt.Errorf("port forward stopped unexpectedly before being ready: %s", errOut.String())
	}
}

func (d *Dumper) getSecretsOfPod(pod corev1.Pod) (*corev1.SecretList, error) {
	secretNames := make(map[string]struct{})
	for _, volume := range pod.Spec.Volumes {
		if volume.Secret != nil {
			secretNames[volume.Secret.SecretName] = struct{}{}
		}
	}

	secretList := &corev1.SecretList{Items: []corev1.Secret{}}
	secretGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}

	for secretName := range secretNames {
		unstructuredSecret, err := d.dynamicClient.Resource(secretGVR).Namespace(pod.Namespace).Get(context.TODO(), secretName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				log.Printf("Warning: secret %s/%s referenced by pod %s not found. Skipping.\n", pod.Namespace, secretName, pod.Name)
				continue
			}
			return nil, fmt.Errorf("error getting secret %s/%s referenced by pod %s: %w", pod.Namespace, secretName, pod.Name, err)
		}

		secret := &corev1.Secret{}
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredSecret.UnstructuredContent(), secret)
		if err != nil {
			return nil, fmt.Errorf("error converting Unstructured to Secret %s: %v", secretName, err)
		}
		secretList.Items = append(secretList.Items, *secret)
	}

	return secretList, nil
}

// Gets logs from all containers inside pod, transforms them into one string and returns it
func (d *Dumper) getLogs(pod corev1.Pod) (string, error) {
	var allLogs strings.Builder
	containers := append(pod.Spec.InitContainers, pod.Spec.Containers...)
	if len(containers) == 0 {
		return "", fmt.Errorf("pod %s in namespace %s has no containers", pod.Name, pod.Namespace)
	}

	for _, container := range containers {
		logOptions := &corev1.PodLogOptions{
			Container: container.Name,
		}
		podLogRequest := d.clientSet.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, logOptions)
		stream, err := podLogRequest.Stream(context.TODO())
		if err != nil {
			return "", fmt.Errorf("error while getting stream from pod %s in namespace %s: %w", pod.Name, pod.Namespace, err)
		}
		defer stream.Close()
		buf := new(strings.Builder)
		_, err = io.Copy(buf, stream)
		if err != nil {
			return "", fmt.Errorf("error while copying logs from pod %s in namespace %s: %w", pod.Name, pod.Namespace, err)
		}
		allLogs.WriteString(fmt.Sprintf("Container: %s Logs:\n", container.Name))
		allLogs.WriteString(buf.String())
		allLogs.WriteString(fmt.Sprintf("\nEnd of %s logs.\n", container.Name))
	}
	return allLogs.String(), nil
}

// FindGVRForShortName uses the discovery client to find the full GroupVersionResource
// for a given short name.
func (d *Dumper) findGVRForShortName(shortName string) (schema.GroupVersionResource, error) {
	resourceLists, err := d.clientSet.Discovery().ServerPreferredResources()
	if err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("failed to get server preferred resources: %w", err)
	}

	for _, list := range resourceLists {
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			log.Printf("Skipping invalid GroupVersion %s: %v", list.GroupVersion, err)
			continue
		}

		for _, res := range list.APIResources {
			if res.Name == shortName {
				return schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: res.Name}, nil
			}

			if slices.Contains(res.ShortNames, shortName) {
				return schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: res.Name}, nil
			}
		}
	}
	return schema.GroupVersionResource{}, fmt.Errorf("resource with short name '%s' not found", shortName)
}

// This function is the same as the findGVRForShortName function, but it was created to differentiate between PG and PGV2 when finding the GVR.
// func (d *Dumper) FindGVRByShortNameAndAPIVersion(shortName, apiGroupVersion string) (schema.GroupVersionResource, error) {
// 	gv, err := schema.ParseGroupVersion(apiGroupVersion)
// 	if err != nil {
// 		return schema.GroupVersionResource{}, errors.Wrapf(err, "invalid apiGroupVersion: %s", apiGroupVersion)
// 	}

// 	resourceList, err := d.clientset.Discovery().ServerResourcesForGroupVersion(apiGroupVersion)
// 	if err != nil {
// 		return schema.GroupVersionResource{}, errors.Wrapf(err, "failed to get server resources for %s", apiGroupVersion)
// 	}

// 	for _, res := range resourceList.APIResources {
// 		if res.Name == shortName {
// 			return schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: res.Name}, nil
// 		}

// 		for _, sn := range res.ShortNames {
// 			if sn == shortName {
// 				// Found it!
// 				return schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: res.Name}, nil
// 			}
// 		}
// 	}

// 	return schema.GroupVersionResource{}, fmt.Errorf("resource with short name or plural name '%s' not found in apiGroupVersion '%s'", shortName, apiGroupVersion)
// }
