package main

import (
	"flag"
	"os"
	"path/filepath"
	"time"

	"github.com/ereOn/k8s-redis-cluster-operator/pkg/k8s"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var kubeconfig = flag.String("kubeconfig", filepath.Join(os.Getenv("HOME"), ".kube", "config"), "absolute path to the kubeconfig file")

func main() {
	flag.Parse()

	config := getConfig()
	clientset := getClientset(config)

	k8s.RegisterThirdPartyResource(clientset)
	defer k8s.UnregisterThirdPartyResource(clientset)
	time.Sleep(time.Second * 10)
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
