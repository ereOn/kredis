package tpr

import (
	"encoding/json"

	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/api/meta"
	"k8s.io/client-go/pkg/api/unversioned"
)

type RedisClusterSpec struct {
	Name string `json:"name"`
	Size int    `json:"size"`
}

type RedisCluster struct {
	unversioned.TypeMeta `json:",inline"`
	Metadata             api.ObjectMeta `json:"metadata"`

	Spec RedisClusterSpec `json:"spec"`
}

type RedisClusterList struct {
	unversioned.TypeMeta `json:",inline"`
	Metadata             unversioned.ListMeta `json:"metadata"`

	Items []RedisCluster `json:"items"`
}

func (rc *RedisCluster) GetObjectKind() unversioned.ObjectKind {
	return &rc.TypeMeta
}

func (rc *RedisCluster) GetObjectMeta() meta.Object {
	return &rc.Metadata
}

func (rcl *RedisClusterList) GetObjectKind() unversioned.ObjectKind {
	return &rcl.TypeMeta
}

func (rcl *RedisClusterList) GetListMeta() unversioned.List {
	return &rcl.Metadata
}

func (rc *RedisCluster) UnmarshalJSON(data []byte) error {
	value := RedisCluster{}
	err := json.Unmarshal(data, &value)

	if err != nil {
		return err
	}

	*rc = value

	return nil
}

func (rcl *RedisClusterList) UnmarshalJSON(data []byte) error {
	value := RedisClusterList{}
	err := json.Unmarshal(data, &value)

	if err != nil {
		return err
	}

	*rcl = value

	return nil
}
