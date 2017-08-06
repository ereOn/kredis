package operator

import (
	"context"

	"github.com/go-kit/kit/log"

	"k8s.io/client-go/rest"
)

// An Operator manages Redis cluster resources in a Kubernetes Cluster.
//
// There should be only one operator running at any given time in a kubernetes
// cluster.
type Operator struct {
	client *rest.RESTClient
	logger log.Logger
}

// New create a new operator.
func New(client *rest.RESTClient, logger log.Logger) *Operator {
	return &Operator{
		client: client,
		logger: logger,
	}
}

// Run start the operator for as long as the specified context lives.
//
// In case of a failure, the methods exits with an error that describes what
// happened.
//
// The context being cancelled is *NOT* considered a failure and causes the
// method to exit with nil.
func (o *Operator) Run(ctx context.Context) error {
	return nil
}
