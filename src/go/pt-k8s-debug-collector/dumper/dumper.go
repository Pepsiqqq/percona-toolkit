package dumper

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.yaml.in/yaml/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	NumWorkers   = 10               // Concurrency for Pod Logs
	MaxRamPerLog = 50 * 1024 * 1024 // 50MB RAM limit before spilling to disk
)

// Dumper struct is for dumping cluster
type Dumper struct {
	kubeconfig     string
	namespace      string
	location       string
	errors         string
	mode           int64
	crType         []string
	forwardport    string
	skipPodSummary bool

	sslSecrets      map[string]bool
	individualFiles []individualFile
	clientSet       *kubernetes.Clientset
	dynamicClient   *dynamic.DynamicClient
	discoveryClient *discovery.DiscoveryClient
	archive         *tarWriter
	restConfig      *rest.Config
}

// individualFile struct is used to dump the necessary files from the containers
type individualFile struct {
	resourceName  string
	containerName string
	filepaths     []string
}

// resourceMap struct is used to dump the resources from namespace scope or cluster scope
type resourceMap struct {
	ClusterScoped   []schema.GroupVersionResource
	NamespaceScoped []schema.GroupVersionResource
}

// exportJob struct is used in goroutines to access pods
type exportJob struct {
	Pod corev1.Pod
}

var resourcesRe = regexp.MustCompile(`(\w+\.(\w+).percona\.com)`)

// New return new Dumper object
func New(location, namespace, kubeconfig, forwardport, resource string, skipPodSummary bool) (*Dumper, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build config from flags: %w", err)
	}

	config.QPS = 10
	config.Burst = 20

	clientset := kubernetes.NewForConfigOrDie(config)
	dynclient := dynamic.NewForConfigOrDie(config)
	discclient := discovery.NewDiscoveryClientForConfigOrDie(config)

	// TODO: implement cluster name flag
	d := &Dumper{
		kubeconfig:      kubeconfig,
		location:        "cluster-dump",
		mode:            int64(0o777),
		namespace:       namespace,
		forwardport:     forwardport,
		skipPodSummary:  skipPodSummary,
		clientSet:       clientset,
		dynamicClient:   dynclient,
		discoveryClient: discclient,
		restConfig:      config,
		crType:          []string{resource},
	}

	if len(d.crType) == 0 || d.crType[0] == "none" {
		return d, nil
	}

	// if the resource is automatic we first need to determine which one we need to use
	// TODO: MAYBE REMOVE OR SIMPLIFY
	if resourceType(d.crType[0]) == "auto" {
		d.crType, err = d.autoCustomResource()
		if err != nil {
			return nil, fmt.Errorf("failed to determine custom resource automatically: %w", err)
		}
	}

	d.sslSecrets = make(map[string]bool, 0)
	for _, types := range d.crType {
		switch resourceType(types) {
		case "pg":
			// TODO: implement
		case "pgv2":
			err := d.addPg()
			if err != nil {
				log.Fatalf("%v", err)
			}
		case "pxc":
			err := d.addPxc()
			if err != nil {
				log.Fatalf("%v", err)
			}
		case "ps":
			err := d.addPs()
			if err != nil {
				log.Fatalf("%v", err)
			}
		case "psmdb":
			err := d.addPsmdb()
			if err != nil {
				log.Fatalf("%v", err)
			}
		}
	}

	return d, nil
}

// DumpCluster create dump of a cluster in Dumper.location
func (d *Dumper) DumpCluster() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var err error
	d.archive, err = NewTarWriter(d.location + ".tar.gz")
	if err != nil {
		log.Fatalf("Failed to create archive: %v", err)
	}
	defer d.archive.Close()

	fmt.Println("Initializing Pod Cache")
	factory := informers.NewSharedInformerFactory(d.clientSet, 10*time.Minute)
	podInformer := factory.Core().V1().Pods().Informer()
	factory.Start(ctx.Done())

	fmt.Println("Discovering and Exporting API Resources")
	if err := d.Export(ctx); err != nil {
		log.Printf("Error during resource export: %v", err)
		d.archive.WriteVirtualFile("Error_Resource.txt", []byte(err.Error()))
	}

	fmt.Println("Starting Workers for Pod Logs/Files...")
	jobsChannel := make(chan exportJob, 100)
	var wg sync.WaitGroup

	for i := range NumWorkers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			d.resilientWorker(id, ctx, cancel, jobsChannel)
		}(i)
	}

	fmt.Println("Waiting for Pod Cache to fully sync...")
	if !cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced) {
		log.Fatal("Timed out waiting for caches to sync.")
	}

	podLister := factory.Core().V1().Pods().Lister()
	allPods, err := podLister.List(labels.Everything())
	if err != nil {
		log.Fatalf("Failed to list all pods: %v", err)
	}

	fmt.Printf("Dispatching %d pods to workers...\n", len(allPods))
	for _, pod := range allPods {
		jobsChannel <- exportJob{Pod: *pod}
	}

	close(jobsChannel)
	wg.Wait()

	fmt.Printf("\nExport Complete. Data saved to %s", d.location)
	return nil
}

func (d *Dumper) Export(ctx context.Context) error {
	resources, err := d.discoverResources()
	if err != nil {
		return err
	}
	fmt.Printf("Found %d Cluster Types and %d Namespaced Types\n", len(resources.ClusterScoped), len(resources.NamespaceScoped))

	var wg sync.WaitGroup
	semCluster := make(chan struct{}, 5)

	for _, gvr := range resources.ClusterScoped {
		wg.Add(1)
		go func(r schema.GroupVersionResource) {
			defer wg.Done()
			semCluster <- struct{}{}
			defer func() { <-semCluster }()

			d.exportGeneric(ctx, r, "", "cluster_scope")
		}(gvr)
	}

	namespaces, err := d.clientSet.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	semNS := make(chan struct{}, 5)

	for _, ns := range namespaces.Items {
		if d.namespace != ns.Namespace {
			continue
		}
		wg.Add(1)
		go func(namespace string) {
			defer wg.Done()
			semNS <- struct{}{}
			defer func() { <-semNS }()

			if err := d.dumpSecrets(ctx, namespace); err != nil {
				log.Printf("Error dumping secrets for %s: %v", namespace, err)
			}

			for _, gvr := range resources.NamespaceScoped {
				if gvr.Resource == "secrets" {
					continue
				}
				d.exportGeneric(ctx, gvr, namespace, namespace)
			}
		}(ns.Name)
	}

	wg.Wait()
	return nil
}

func (d *Dumper) exportGeneric(ctx context.Context, gvr schema.GroupVersionResource, ns string, parentDir string) {
	var intf dynamic.ResourceInterface
	if ns == "" {
		intf = d.dynamicClient.Resource(gvr)
	} else {
		intf = d.dynamicClient.Resource(gvr).Namespace(ns)
	}

	list, err := intf.List(ctx, metav1.ListOptions{})
	if err != nil || len(list.Items) == 0 {
		return
	}

	for _, item := range list.Items {
		name := item.GetName()

		unstructured := item.Object
		// TODO: CHECK WHAT FIELDS COULD BE ERASED
		// if meta, ok := unstructured["metadata"].(map[string]interface{}); ok {
		// 	delete(meta, "managedFields")
		// 	delete(meta, "resourceVersion")
		// 	delete(meta, "uid")
		// }

		data, _ := yaml.Marshal(unstructured)

		yamlPath := fmt.Sprintf("%s/%s/%s.yaml", parentDir, gvr.Resource, name)
		d.archive.WriteVirtualFile(yamlPath, data)
	}
}

func (d *Dumper) discoverResources() (*resourceMap, error) {
	lists, _ := d.discoveryClient.ServerPreferredResources()
	rm := &resourceMap{}

	ignoredResources := map[string]bool{
		"componentstatuses": true, // Deprecated
		"endpoints":         true, // Deprecated
		"events":            true, // Too noisy
		"pods":              true, // Handled by workers
	}

	for _, list := range lists {
		gv, _ := schema.ParseGroupVersion(list.GroupVersion)
		for _, resource := range list.APIResources {
			if strings.Contains(resource.Name, "/") {
				continue
			}

			if ignoredResources[resource.Name] {
				continue
			}

			gvr := schema.GroupVersionResource{
				Group:    gv.Group,
				Version:  gv.Version,
				Resource: resource.Name,
			}
			if resource.Namespaced {
				rm.NamespaceScoped = append(rm.NamespaceScoped, gvr)
			} else {
				rm.ClusterScoped = append(rm.ClusterScoped, gvr)
			}
		}
	}
	return rm, nil
}

func (d *Dumper) resilientWorker(id int, ctx context.Context, cancel context.CancelFunc, jobs <-chan exportJob) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}

			logPath := fmt.Sprintf("%s/%s/", job.Pod.Namespace, job.Pod.Name)
			err := d.exportPodLogs(ctx, job.Pod, logPath)
			if err != nil {
				if isSpaceError(err) {
					fmt.Printf("Worker %d stopping app: %v\n", id, err)
					cancel()
					return
				}
				report := fmt.Sprintf("Error exporting logs: %v", err)
				d.archive.WriteVirtualFile(fmt.Sprintf("%s/%s/ERRORS.txt", job.Pod.Namespace, job.Pod.Name), []byte(report))
			}

			if job.Pod.Status.Phase == corev1.PodRunning {
				d.exportPodSummaryAndFiles(job)
			}
		}
	}
}

func (d *Dumper) exportPodSummaryAndFiles(job exportJob) {
	for index := range d.crType {
		crname := d.crType[index]
		if crname == "psmdb" {
			crname = "mongod"
		}
		if crname == "ps" {
			crname = "mysql"
		}
		if job.Pod.Labels["app.kubernetes.io/component"] == crname ||
			job.Pod.Labels["app.kubernetes.io/name"] == crname ||
			(crname == "pg" && job.Pod.Labels["pgo-pg-database"] == "true") ||
			(crname == "pgv2" && job.Pod.Labels["pgv2.percona.com/version"] != "" && job.Pod.Labels["postgres-operator.crunchydata.com/instance"] != "") {

			location := filepath.Join(d.location, job.Pod.Namespace, job.Pod.Name, "/summary.txt")

			// Get summary
			if !d.skipPodSummary {
				d.getSummary(job, d.crType[index], location)
			}

			// Get individual files
			d.getIndividualFiles(job, d.crType[index])
		}
	}
}

func isSpaceError(err error) bool {
	return strings.Contains(err.Error(), "no space left on device")
}

func (d *Dumper) logError(err error) {
	log.Printf("%v", err)
	d.errors += fmt.Sprintf("%v \n\n", err)
}
