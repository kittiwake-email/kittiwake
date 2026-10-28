package storage

import (
	"context"
	"errors"
	"testing"

	"kittiwake/rados"
)

type noOmap struct{ rados.IOContext }

func (p noOmap) OmapGet(context.Context, string, []string) (map[string][]byte, error) {
	return nil, errors.New("omap unsupported")
}

func TestPoolCapabilitiesAndCleanup(t *testing.T) {
	ctx := context.Background()
	client := rados.NewMemoryClient()
	io, _ := client.Pool("test")
	if err := probePool(ctx, noOmap{io}, RoleIndex); err == nil {
		t.Fatal("index accepted without OMAP")
	}
	if err := probePool(ctx, noOmap{io}, RoleAttachments); err == nil {
		t.Fatal("attachments accepted without OMAP")
	}
	if err := probePool(ctx, noOmap{io}, RoleMessages); err != nil {
		t.Fatal(err)
	}
	if err := probePool(ctx, io, RoleIndex); err != nil {
		t.Fatal(err)
	}
	count := 0
	if err := io.List(ctx, func(string) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("left %d probe objects", count)
	}
}
