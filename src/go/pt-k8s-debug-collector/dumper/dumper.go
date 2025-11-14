package dumper

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// sslSecret struct is used for dumping certificates.
type sslSecret struct {
	secretTemplate string
	secretGVR      schema.GroupVersionResource
	secretDataName []string
}

// individualFile struct is used to dump the necessary files from the containers
type individualFile struct {
	containerName string
	filepaths     []string
}

// Dumper struct is for dumping cluster
type Dumper struct {
	kubeconfig     string
	resourcesMap   []schema.GroupVersionResource
	namespace      string
	location       string
	errors         string
	mode           int64
	crType         string
	forwardport    string
	sslSecrets     []sslSecret
	skipPodSummary bool

	individualFiles []individualFile
	clientSet       *kubernetes.Clientset
	dynamicClient   *dynamic.DynamicClient
	restConfig      *rest.Config
	tw              *tar.Writer
}

var resourcesRe = regexp.MustCompile(`(\w+\.(\w+).percona\.com)`)

// New return new Dumper object
func New(location, namespace, kubeconfig, forwardport, resource string, skipPodSummary bool) (*Dumper, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build config from flags: %w", err)
	}
	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create client set: %w", err)
	}

	d := &Dumper{
		kubeconfig:     kubeconfig,
		location:       "cluster-dump",
		mode:           int64(0o777),
		namespace:      namespace,
		forwardport:    forwardport,
		skipPodSummary: skipPodSummary,
		clientSet:      clientSet,
		dynamicClient:  dynClient,
		restConfig:     config,
		crType:         resource,
	}

	d.resourcesMap = []schema.GroupVersionResource{
		{
			Group:    "",
			Version:  "v1",
			Resource: "pods",
		},
		{
			Group:    "",
			Version:  "v1",
			Resource: "replicationcontrollers",
		},
		{
			Group:    "",
			Version:  "v1",
			Resource: "events",
		},
		{
			Group:    "",
			Version:  "v1",
			Resource: "configmaps",
		},
		{
			Group:    "",
			Version:  "v1",
			Resource: "persistentvolumeclaims",
		},
		{
			Group:    "",
			Version:  "v1",
			Resource: "persistentvolumes",
		},
		{
			Group:    "apps",
			Version:  "v1",
			Resource: "replicasets",
		},
		{
			Group:    "apps",
			Version:  "v1",
			Resource: "deployments",
		},
		{
			Group:    "apps",
			Version:  "v1",
			Resource: "statefulsets",
		},
		{
			Group:    "batch",
			Version:  "v1",
			Resource: "cronjobs",
		},
		{
			Group:    "batch",
			Version:  "v1",
			Resource: "jobs",
		},
		{
			Group:    "policy",
			Version:  "v1",
			Resource: "poddisruptionbudgets",
		},
		{
			Group:    "rbac.authorization.k8s.io",
			Version:  "v1",
			Resource: "clusterrolebindings",
		},
		{
			Group:    "rbac.authorization.k8s.io",
			Version:  "v1",
			Resource: "clusterroles",
		},
		{
			Group:    "rbac.authorization.k8s.io",
			Version:  "v1",
			Resource: "rolebindings",
		},
		{
			Group:    "rbac.authorization.k8s.io",
			Version:  "v1",
			Resource: "roles",
		},
		{
			Group:    "storage.k8s.io",
			Version:  "v1",
			Resource: "storageclasses",
		},
	}
	d.sslSecrets = make([]sslSecret, 0)

	// if the resource is automatic we first need to determine which one we need to use
	if resourceType(d.crType) == "auto" {
		d.crType, err = d.autoCustomResource()
		if err != nil {
			return nil, fmt.Errorf("failed to determine custom resource automatically: %w", err)
		}
	}

	switch resourceType(d.crType) {
	case "pg":
		// resources = append(resources,
		// 	"perconapgclusters.pg.percona.com",
		// 	"pgclusters.pg.percona.com",
		// 	"pgpolicies.pg.percona.com",
		// 	"pgreplicas.pg.percona.com",
		// 	"pgtasks.pg.percona.com",
		// )

		// sslSecrets = append(sslSecrets,
		// 	sslSecret{
		// 		secret:    "{{ .Name }}-ssl-ca",
		// 		resource:  "perconapgclusters.pg.percona.com",
		// 		dataNames: []string{"ca.crt"},
		// 	},
		// 	sslSecret{
		// 		secret:    "{{ .Name }}-ssl-keypair",
		// 		resource:  "perconapgclusters.pg.percona.com",
		// 		dataNames: []string{"tls.crt"},
		// 	},
		// 	sslSecret{
		// 		secret:    "{{ .Name }}-replication-ssl-keypair",
		// 		resource:  "perconapgclusters.pg.percona.com",
		// 		dataNames: []string{"tls.crt"},
		// 	},
		// 	sslSecret{
		// 		secret:    "pgo.tls",
		// 		resource:  "perconapgclusters.pg.percona.com",
		// 		dataNames: []string{"tls.crt"},
		// 	},
		// )
	case "pgv2":
		d.resourcesMap = append(d.resourcesMap, []schema.GroupVersionResource{
			{
				Group:    "pgv2.percona.com",
				Version:  "v2",
				Resource: "perconapgbackups",
			},
			{
				Group:    "pgv2.percona.com",
				Version:  "v2",
				Resource: "perconapgclusters",
			},
			{
				Group:    "pgv2.percona.com",
				Version:  "v2",
				Resource: "perconapgrestores",
			},
		}...)

		gvr, err := d.findGVRForShortName("pg")
		if err != nil {
			log.Fatalf("error getting gvr from short name: %v", err)
		}
		d.sslSecrets = append(d.sslSecrets,
			sslSecret{
				secretTemplate: "{{ .Name }}-cluster-cert",
				secretGVR:      gvr,
				secretDataName: []string{"ca.crt", "tls.crt"},
			},
			sslSecret{
				secretTemplate: "pgo-root-cacert",
				secretGVR:      gvr,
				secretDataName: []string{"root.crt"},
			})

	case "pxc":
		d.resourcesMap = append(d.resourcesMap, []schema.GroupVersionResource{
			{
				Group:    "pxc.percona.com",
				Version:  "v1",
				Resource: "perconaxtradbclusterbackups",
			},
			{
				Group:    "pxc.percona.com",
				Version:  "v1",
				Resource: "perconaxtradbclusterrestores",
			},
			{
				Group:    "pxc.percona.com",
				Version:  "v1",
				Resource: "perconaxtradbclusters",
			},
		}...)

		filepaths := []string{
			"var/lib/mysql/mysqld-error.log",
			"var/lib/mysql/innobackup.backup.log",
			"var/lib/mysql/innobackup.move.log",
			"var/lib/mysql/innobackup.prepare.log",
			"var/lib/mysql/grastate.dat",
			"var/lib/mysql/gvwstate.dat",
			"var/lib/mysql/mysqld.post.processing.log",
			"var/lib/mysql/auto.cnf",
		}

		d.individualFiles = append(d.individualFiles, individualFile{
			containerName: "logs",
			filepaths:     filepaths,
		})

		gvr, err := d.findGVRForShortName("pxc")
		if err != nil {
			log.Fatalf("error getting gvr from short name: %v", err)
		}
		d.sslSecrets = append(d.sslSecrets,
			sslSecret{
				secretTemplate: "{{ .Name }}-ssl",
				secretGVR:      gvr,
				secretDataName: []string{"ca.crt", "tls.crt"},
			},
			sslSecret{
				secretTemplate: "{{ .Name }}-ssl-internal",
				secretGVR:      gvr,
				secretDataName: []string{"ca.crt", "tls.crt"},
			},
			sslSecret{
				secretTemplate: "{{ .Name }}-ca-cert",
				secretGVR:      gvr,
				secretDataName: []string{"ca.crt", "tls.crt"},
			})

	case "ps":
		d.resourcesMap = append(d.resourcesMap, []schema.GroupVersionResource{
			{
				Group:    "ps.percona.com",
				Version:  "v1",
				Resource: "perconaservermysqlbackups",
			},
			{
				Group:    "ps.percona.com",
				Version:  "v1",
				Resource: "perconaservermysqlrestores",
			},
			{
				Group:    "ps.percona.com",
				Version:  "v1",
				Resource: "perconaservermysqls",
			},
		}...)

		gvr, err := d.findGVRForShortName("ps")
		if err != nil {
			log.Fatalf("error getting gvr from short name: %v", err)
		}
		d.sslSecrets = append(d.sslSecrets,
			sslSecret{
				secretTemplate: "{{ .Name }}-ssl",
				secretGVR:      gvr,
				secretDataName: []string{"ca.crt", "tls.crt"},
			},
			sslSecret{
				secretTemplate: "{{ .Name }}-ca-cert",
				secretGVR:      gvr,
				secretDataName: []string{"ca.crt", "tls.crt"},
			})
	case "psmdb":
		d.resourcesMap = append(d.resourcesMap, []schema.GroupVersionResource{
			{
				Group:    "psmdb.percona.com",
				Version:  "v1",
				Resource: "perconaservermongodbbackups",
			},
			{
				Group:    "psmdb.percona.com",
				Version:  "v1",
				Resource: "perconaservermongodbrestores",
			},
			{
				Group:    "psmdb.percona.com",
				Version:  "v1",
				Resource: "perconaservermongodbs",
			},
		}...)
		gvr, err := d.findGVRForShortName("psmdb")
		if err != nil {
			log.Fatalf("error getting gvr from short name: %v", err)
		}
		d.sslSecrets = append(d.sslSecrets,
			sslSecret{
				secretTemplate: "{{ .Name }}-ssl",
				secretGVR:      gvr,
				secretDataName: []string{"ca.crt", "tls.crt"},
			},
			sslSecret{
				secretTemplate: "{{ .Name }}-ssl-internal",
				secretGVR:      gvr,
				secretDataName: []string{"ca.crt", "tls.crt"},
			},
			sslSecret{
				secretTemplate: "{{ .Name }}-ca-cert",
				secretGVR:      gvr,
				secretDataName: []string{"ca.crt", "tls.crt"},
			})
	}
	return d, nil
}

// DumpCluster create dump of a cluster in Dumper.location
func (d *Dumper) DumpCluster() error {
	file, err := os.Create(d.location + ".tar.gz")
	if err != nil {
		return fmt.Errorf("create tar file: %w", err)
	}

	zr := gzip.NewWriter(file)
	tw := tar.NewWriter(zr)
	d.tw = tw
	defer func() {
		err = d.writeDataToDump([]byte(d.errors), d.location+"/errors.txt")
		if err != nil {
			log.Println("Error: add errors.txt to archive:", err)
		}

		err = d.tw.Close()
		if err != nil {
			log.Println("close tar writer", err)
			return
		}
		err = zr.Close()
		if err != nil {
			log.Println("close gzip writer", err)
			return
		}
		err = file.Close()
		if err != nil {
			log.Println("close file", err)
			return
		}
	}()

	nss := &corev1.NamespaceList{}
	if len(d.namespace) > 0 {
		ns := corev1.Namespace{}
		ns.Name = d.namespace
		nss.Items = append(nss.Items, ns)
	} else {
		nss, err = d.getNamespacesList()
		if err != nil {
			return fmt.Errorf("failed to get namespaces: %w", err)
		}
	}

	for _, ns := range nss.Items {
		podList, err := d.getPodList(ns.Name)
		if err != nil {
			d.logError(fmt.Errorf("error getting pods from \"%s\" namespace: %w", ns.Name, err))
			continue
		}
		for _, pod := range podList.Items {
			err := d.writeSecretsOfPod(pod)
			if err != nil {
				d.logError(fmt.Errorf("error getting secrets from \"%s\" namespace: %w", ns.Name, err))
			}

			err = d.writeLogsFromPod(pod)
			if err != nil {
				d.logError(fmt.Errorf("error while writing logs from pods and \"%s\" namespace to dump: %w", ns.Name, err))
			}

			if len(pod.Labels) == 0 {
				continue
			}

			component := resourceType(d.crType)
			if component == "psmdb" {
				component = "mongod"
			}
			if component == "ps" {
				component = "mysql"
			}
			if pod.Labels["app.kubernetes.io/component"] == component ||
				pod.Labels["app.kubernetes.io/name"] == component ||
				(component == "pg" && pod.Labels["pgo-pg-database"] == "true") ||
				(component == "pgv2" && pod.Labels["pgv2.percona.com/version"] != "" && pod.Labels["postgres-operator.crunchydata.com/instance"] != "") {

				location := filepath.Join(d.location, ns.Name, pod.Name, "/summary.txt")
				//Get summary
				if !d.skipPodSummary {
					output, err := d.getPodSummary(pod)
					if err != nil {
						d.logError(fmt.Errorf("error while creating summary for \"%s\" pod and \"%s\" namespace: %w", pod.Name, ns.Name, err))
						err = d.writeDataToDump([]byte(err.Error()), location)
						if err != nil {
							log.Printf("Error: create summary errors archive for pod %s in namespace %s: %v", pod.Name, ns.Name, err)
						}
					} else {
						log.Printf("Created summary for pod/namespace \"%s\"/\"%s\", Writing to dump", pod.Name, pod.Namespace)
						err = d.writeDataToDump(output, location)
						if err != nil {
							d.logError(fmt.Errorf("error while writing summary for \"%s\" pod and \"%s\" namespace to dump: %w", pod.Name, ns.Name, err))
						}
					}
				}

				// get individual files(Logs)
				location = filepath.Join(d.location, ns.Name, pod.Name)
				for _, indf := range d.individualFiles {
					for _, path := range indf.filepaths {
						file, err := d.getIndividualFilesFromPod(pod, path, indf.containerName)
						if err != nil {
							d.logError(fmt.Errorf("error while getting individual files for \"%s\" pod and \"%s\" namespace to dump: %w", pod.Name, ns.Name, err))
							continue
						}

						if len(file) != 0 {
							log.Printf("Writing individual file with path %s to dump", path)
							err := d.writeDataToDump(file, location+"/"+path)
							if err != nil {
								d.logError(fmt.Errorf("error while writing individula files for \"%s\" pod and \"%s\" namespace to dump: %w", pod.Name, ns.Name, err))
							}
						}
					}
				}

			}
		}
		for _, gvr := range d.resourcesMap {
			data, err := d.getResource(gvr)
			if err != nil {
				d.logError(fmt.Errorf("error while getting resource \"%s\" in \"%s\" namespace: %w", gvr.Resource, ns.Name, err))
				continue
			}
			err = d.writeDataToDump(data, filepath.Join(d.location, ns.Name, gvr.Resource+".yaml"))
			if err != nil {
				d.logError(fmt.Errorf("error while dumping resource \"%s\" in \"%s\" namespace: %w", gvr.Resource, ns.Name, err))
			}
		}
		for _, ssl := range d.sslSecrets {
			err = d.dumpSSLDataFromSecrets(ns.Name, ssl)
			if err != nil {
				d.logError(fmt.Errorf("error while dumping ssl data in \"%s\" namespace: %w", ns.Name, err))
			}
		}
	}

	err = d.writeNodes()
	if err != nil {
		d.logError(fmt.Errorf("error while dumping nodes: %w", err))
	}

	return nil
}

func (d *Dumper) writeLogsFromPod(pod corev1.Pod) error {
	logs, err := d.getLogs(pod)
	if err != nil {
		return fmt.Errorf("error while getting logs from pod \"%s\" in namespace \"%s\": %w", pod.Name, pod.Namespace, err)
	}
	location := filepath.Join(d.location, pod.Namespace, pod.Name, "logs.txt")
	log.Printf("Found logs in pod \"%s\" in namespace \"%s\". Writing to the dump\n", pod.Name, pod.Namespace)
	err = d.writeDataToDump([]byte(logs), location)
	if err != nil {
		return fmt.Errorf("error while adding logs from pod \"%s\" in namespace \"%s\" to dump: %w", pod.Name, pod.Namespace, err)
	}
	return nil
}

func (d *Dumper) writeSecretsOfPod(pod corev1.Pod) error {
	location := filepath.Join(d.location, pod.Namespace)
	secretList, err := d.getSecretsOfPod(pod)
	if err != nil {
		return fmt.Errorf("failed to get secrets from namespace: %w", err)
	}
	for _, secret := range secretList.Items {
		secretName := secret.GetName()
		itemMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&secret)
		if err != nil {
			return fmt.Errorf("error converting secret %s to map: %w", secretName, err)
		}

		// pt-k8s-debug-collector should not collect secret data of pgbouncer
		if strings.Contains(secretName, "pgbouncer") {
			log.Printf("Cleaning secret data from \"%s\" in namespace \"%s\".\n", secret.GetName(), pod.Namespace)
			itemMap["data"] = map[string]interface{}{
				"warning": "pt-k8s-debug-collector is not collecting secret details of pgbouncer",
			}
		}

		yamlBytes, err := yaml.Marshal(itemMap)
		if err != nil {
			return fmt.Errorf("failed to marshal object to YAML: %w", err)
		}

		log.Printf("Found secret with name/namespace \"%s\"/\"%s\". Writing to dump.\n", secretName, pod.Namespace)
		err = d.writeDataToDump(yamlBytes, filepath.Join(location, "secret/"+secretName+".yaml"))
		if err != nil {
			return fmt.Errorf("failed to write secret to dump: %w", err)
		}
	}
	return nil
}

func (d *Dumper) writeNodes() error {
	nodeList, err := d.getNodeList()
	if err != nil {
		return fmt.Errorf("error while getting nodes: %w", err)
	}
	var buf bytes.Buffer
	for _, node := range nodeList.Items {
		node.ManagedFields = nil
		yamlBytes, err := yaml.Marshal(node)
		if err != nil {
			return fmt.Errorf("failed to marshal object to YAML: %w", err)
		}
		_, err = buf.Write(yamlBytes)
		if err != nil {
			return fmt.Errorf("failed to add data to buffer: %w", err)
		}
	}
	log.Print("Found nodes. Writing to the dump\n")
	location := filepath.Join(d.location, "nodes.yaml")
	err = d.writeDataToDump(buf.Bytes(), location)
	if err != nil {
		return fmt.Errorf("error while adding nodes to dump: %w", err)
	}

	return nil
}

func (d *Dumper) getIndividualFilesFromPod(pod corev1.Pod, filepath, containerName string) ([]byte, error) {
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

func (d *Dumper) getPodSummary(pod corev1.Pod) ([]byte, error) {
	var (
		summCmdName string
		ports       string
		summCmdArgs []string
	)

	switch resourceType(d.crType) {
	case "pxc", "ps":
		var port string
		if d.forwardport != "" {
			port = d.forwardport
		} else {
			port = "3306"
		}

		pass, err := d.getSecretValueFromPod(pod, "root")
		if err != nil {
			return nil, fmt.Errorf("failed to get password from pxc/ps users secret: %w", err)
		}

		ports = port + ":3306"
		summCmdName = "pt-mysql-summary"
		summCmdArgs = []string{"--host=127.0.0.1", "--port=" + port, "--user=root", "--password=" + pass}

	case "pgv2":
		scriptURL := "https://raw.githubusercontent.com/percona/support-snippets/master/postgresql/pg_gather/gather.sql"
		resp, err := http.Get(scriptURL)
		if err != nil {
			return nil, fmt.Errorf("error fetching SQL script: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("failed to fetch SQL script, status code: %d", resp.StatusCode)
		}
		command := []string{"psql", "-X", "-f", "-"}

		outb, errb, err := d.executeInPod(resp.Body, command, pod, "database")
		if err != nil {
			return nil, fmt.Errorf("failed to execute command inside pod stdout: %s\n, stderr\n: %s: %w", outb.String(), errb.String(), err)
		}
		return outb.Bytes(), nil

	case "psmdb":
		var port string
		if d.forwardport != "" {
			port = d.forwardport
		} else {
			port = "27017"
		}

		user, err := d.getSecretValueFromPod(pod, "MONGODB_DATABASE_ADMIN_USER")
		if err != nil {
			return nil, fmt.Errorf("get user name from psmdb users secret: %w", err)
		}
		pass, err := d.getSecretValueFromPod(pod, "MONGODB_DATABASE_ADMIN_PASSWORD")
		if err != nil {
			return nil, fmt.Errorf("get password from psmdb users secret: %w", err)
		}

		ports = port + ":27017"
		summCmdName = "pt-mongodb-summary"
		summCmdArgs = []string{"--username=" + user, "--password=" + string(pass), "--authenticationDatabase=admin", "127.0.0.1:" + port}
	}
	stopChan, err := d.portForwardPod(pod, []string{ports})
	if err != nil {
		return nil, err
	}
	defer close(stopChan)

	var outb, errb bytes.Buffer
	cmd := exec.Command(summCmdName, summCmdArgs...)
	cmd.Stdout = &outb
	cmd.Stderr = &errb
	err = cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("stderr: %s\nstdout: %s \nerr: %w", errb.String(), outb.String(), err)
	}
	return outb.Bytes(), nil
}

func resourceType(s string) string {
	if s == "auto" {
		return "auto"
	} else if s == "pxc" || strings.HasPrefix(s, "pxc/") {
		return "pxc"
	} else if s == "psmdb" || strings.HasPrefix(s, "psmdb/") {
		return "psmdb"
	} else if s == "pg" || strings.HasPrefix(s, "pg/") {
		return "pg"
	} else if s == "pgv2" || strings.HasPrefix(s, "pgv2/") {
		return "pgv2"
	} else if s == "ps" || strings.HasPrefix(s, "ps/") {
		return "ps"
	}
	return s
}

func (d *Dumper) dumpSSLDataFromSecrets(namespace string, secret sslSecret) error {
	t, err := template.New("secret").Parse(secret.secretTemplate)
	if err != nil {
		return fmt.Errorf("error whlie parse secret template: %w", err)
	}

	list, err := d.getUnstructuredListWithNamespace(secret.secretGVR, namespace)
	if err != nil {
		return fmt.Errorf("error while listening resources for %s in namespace %s: %w", secret.secretGVR.String(), namespace, err)
	}

	if len(list.Items) == 0 {
		log.Printf("No resources found for %s in namespace %s", secret.secretGVR.String(), namespace)
		return nil
	}

	for _, item := range list.Items {
		itemName := item.GetName()
		location := d.location
		if len(namespace) > 0 {
			location = filepath.Join(d.location, namespace)
		}

		var nb bytes.Buffer
		templateData := struct {
			Name string
		}{
			Name: itemName,
		}

		if err := t.Execute(&nb, templateData); err != nil {
			log.Printf("Error executing secret template for item %s: %v. Skipping.", itemName, err)
			continue
		}
		secretName := nb.String()
		location = filepath.Join(location, secretName)

		result := make([]byte, 0)

		for _, dn := range secret.secretDataName {
			result = append(result, dn+"\n"...)

			secret, err := d.clientSet.CoreV1().Secrets(namespace).Get(context.TODO(), secretName, metav1.GetOptions{})
			if err != nil {
				log.Printf("Error getting secret %s in namespace %s: %v", secretName, namespace, err)
				result = append(result, []byte(fmt.Sprintf("ERROR: could not get secret: %v\n", err))...)
				continue
			}

			dataBytes, ok := secret.Data[dn]
			if !ok {
				log.Printf("Data key %s not found in secret %s", dn, secretName)
				result = append(result, []byte(fmt.Sprintf("ERROR: key %s not found in secret\n", dn))...)
				continue
			}

			var outb, errb bytes.Buffer
			cmd := exec.Command("openssl", "x509", "-noout", "-text")
			cmd.Stdin = bytes.NewReader(dataBytes)
			cmd.Stdout = &outb
			cmd.Stderr = &errb
			cmd.Env = os.Environ()

			err = cmd.Run()
			if err != nil {
				log.Printf("openssl command failed for %s/%s (key %s): %v. Stderr: %s",
					namespace, secretName, dn, err, errb.String())
				errMsg := fmt.Sprintf("ERROR running openssl: %v\nStderr: %s\n", err, errb.String())
				result = append(result, []byte(errMsg)...)
			} else {
				result = append(result, outb.Bytes()...)
				result = append(result, "\n"...)
			}
		}
		log.Printf("Found ssl data %s. Writing to dump.", itemName)
		err = d.writeDataToDump(result, location)
		if err != nil {
			return fmt.Errorf("cannot add certificates from secret %s to archive: %w", secretName, err)
		}
	}

	return nil
}

func (d *Dumper) autoCustomResource() (string, error) {
	apiGroupList, err := d.clientSet.DiscoveryClient.ServerGroups()
	if err != nil {
		return "", fmt.Errorf("error getting server groups: %w", err)
	}
	var resourceNames string
	var beforeSorting []string
	uniqueResourceNames := make(map[string]bool)
	for _, group := range apiGroupList.Groups {
		for _, version := range group.Versions {
			resourceList, err := d.clientSet.DiscoveryClient.ServerResourcesForGroupVersion(version.GroupVersion)
			if err != nil {
				log.Printf("Warning: Could not get resources for GroupVersion %s: %v", version.GroupVersion, err)
				continue
			}
			for _, resource := range resourceList.APIResources {
				if resource.Name != "" && !strings.Contains(resource.Name, "/") {
					if group.Name != "" {
						uniqueResourceNames[resource.Name+"."+group.Name] = true
					} else {
						uniqueResourceNames[resource.Name] = true
					}
				}
			}
		}
	}
	for name := range uniqueResourceNames {
		beforeSorting = append(beforeSorting, name)
	}
	slices.Sort(beforeSorting)
	for _, name := range beforeSorting {
		resourceNames = resourceNames + name + "\n"
	}

	matches := resourcesRe.FindAllStringSubmatch(resourceNames, -1)
	if len(matches) == 0 {
		return "none", nil
	}
	for _, match := range matches {
		return match[1], nil
	}
	return "", nil
}

func (d *Dumper) logError(err error) {
	log.Printf("%v", err)
	d.errors += fmt.Sprintf("%v \n\n", err)
}

// Writes all data to location in dump
func (d *Dumper) writeDataToDump(data []byte, location string) error {
	hdr := &tar.Header{
		Name:    location,
		Mode:    d.mode,
		ModTime: time.Now(),
		Size:    int64(len(data)),
	}
	if err := d.tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("failed to write header to %s: %w", location, err)
	}
	_, err := d.tw.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write data to %s: %w", location, err)
	}
	return nil
}
