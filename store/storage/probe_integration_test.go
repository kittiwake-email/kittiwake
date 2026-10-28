//go:build integration

package storage

import (
	"context"
	"os"
	"testing"

	"kittiwake/rados"
)

func TestCephPoolCapabilities(t *testing.T) {
	conf := os.Getenv("KITTIWAKE_TEST_CEPH_CONF")
	if conf == "" {
		t.Skip("set KITTIWAKE_TEST_CEPH_CONF")
	}
	client, err := rados.Connect(rados.Config{ConfigFile: conf, User: "kittiwake"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	capacity, err := client.(rados.CapacityProvider).Capacity()
	if err != nil || capacity.TotalBytes == 0 || capacity.AvailableBytes > capacity.TotalBytes {
		t.Fatalf("cluster capacity: %+v %v", capacity, err)
	}
	for i, name := range []string{"kittiwake.index", "kittiwake.messages", "kittiwake.attachments"} {
		io, err := client.Pool(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := probePool(context.Background(), io, Role(i)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}
