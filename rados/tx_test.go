package rados

import (
	"encoding/json"
	"testing"
)

func TestJournalRejectsUnknownOperation(t *testing.T) {
	for _, data := range []string{`[{"kind":-1}]`, `[{"kind":999}]`} {
		var tx WriteTx
		if err := json.Unmarshal([]byte(data), &tx); err == nil {
			t.Fatalf("accepted %s", data)
		}
	}
}
