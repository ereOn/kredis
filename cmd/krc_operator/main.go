package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/ereOn/k8s-redis-cluster-operator/pkg/k8s"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var kubeconfig = flag.String("kubeconfig", filepath.Join(os.Getenv("HOME"), ".kube", "config"), "absolute path to the kubeconfig file")

func main() {
	flag.Parse()

	config := getConfig()
	clientset := getClientset(config)

	success, err := k8s.RegisterRedisClusterResource(clientset)
	must(err)

	if success {
		log.Printf("Registered third-party resource: %s\n", k8s.RedisClusterResourceName)
	} else {
		log.Printf("Third-party resource already registered: %s\n", k8s.RedisClusterResourceName)
	}

	var tprconfig *rest.Config
	tprconfig = config
	k8s.ConfigureClient(tprconfig)

	tprclient, err := rest.RESTClientFor(tprconfig)
	must(err)

	redisCluster, err := k8s.CreateRedisCluster(tprclient, api.NamespaceDefault, &k8s.RedisCluster{
		Metadata: metav1.ObjectMeta{
			Name: "foo",
		},
		Spec: k8s.RedisClusterSpec{
			NodesCount: 3,
		},
	})
	must(err)

	log.Printf("%v", redisCluster)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func getConfig() *rest.Config {
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)

	if err != nil {
		panic(err)
	}

	return config
}

func getClientset(config *rest.Config) *kubernetes.Clientset {
	clientset, err := kubernetes.NewForConfig(config)

	if err != nil {
		panic(err)
	}

	return clientset
}
