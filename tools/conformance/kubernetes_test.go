package conformance

import (
	"context"
	"testing"

	"go.etcd.io/etcd/client/v3/kubernetes"
	"k8s.io/apimachinery/pkg/api/apitesting"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
)

func TestPinnedKubernetesStorageCases(t *testing.T) {
	cases := map[string]func(context.Context, *testing.T, storage.Interface){
		"get":               storagetesting.RunTestGet,
		"create-existing":   storagetesting.RunTestCreateWithKeyExist,
		"delete":            storagetesting.RunTestUnconditionalDelete,
		"delete-conditions": storagetesting.RunTestConditionalDelete,
		"list-paging":       storagetesting.RunTestListPaging,
		"list-prefix":       storagetesting.RunTestGetListRecursivePrefix,
		"update-conflict":   storagetesting.RunTestGuaranteedUpdateWithConflict,
		"create-ttl":        storagetesting.RunTestCreateWithTTL,
		"update-ttl":        storagetesting.RunTestGuaranteedUpdateWithTTL,
		"key-schema":        storagetesting.RunTestKeySchema,
	}
	for name, run := range cases {
		t.Run(name, func(t *testing.T) {
			client := startReference(t, "etcqlite")
			kc := &kubernetes.Client{Client: client}
			kc.Kubernetes = kc
			scheme := runtime.NewScheme()
			metav1.AddToGroupVersion(scheme, metav1.SchemeGroupVersion)
			if err := example.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := examplev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			codec := apitesting.TestCodec(serializer.NewCodecFactory(scheme), examplev1.SchemeGroupVersion)
			versioner := storage.APIObjectVersioner{}
			leases := etcd3.NewDefaultLeaseManagerConfig()
			leases.ReuseDurationSeconds = 1
			store, err := etcd3.New(kc, nil, codec,
				func() runtime.Object { return &example.Pod{} },
				func() runtime.Object { return &example.PodList{} },
				"", "/pods/", schema.GroupResource{Resource: "pods"},
				storagetesting.NewPrefixTransformer([]byte("mock-prefix:"), false),
				leases, etcd3.NewDefaultDecoder(codec, versioner), versioner)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(store.Close)
			run(t.Context(), t, store)
		})
	}
}
