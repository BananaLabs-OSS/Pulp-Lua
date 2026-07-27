package orchestrator

import (
	"fmt"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
	"github.com/vmihailenco/msgpack/v5"
)

func TestFleetArchiveWorkflowEventMatrixUsesExactProviders(t *testing.T) {
	tests := []struct {
		event    string
		provider string
		request  map[string]any
	}{
		{"fleet.workflow.world.download.expire.v1", "fleet.v1.command.world.download.expire", map[string]any{"id": "world-expire-1", "world_id": "world-1", "now": "2026-07-25T18:00:00Z"}},
		{"fleet.workflow.world.download.expire.apply.v1", "fleet.v1.command.world.download.expire.apply", map[string]any{"id": "world-apply-1", "effect_id": "effect-1"}},
		{"fleet.workflow.upload.pending.expire.v1", "fleet.v1.command.upload.pending.expire", map[string]any{"id": "upload-expire-1", "upload_id": "upload-1", "now": "2026-07-25T18:00:00Z"}},
		{"fleet.workflow.upload.pending.expire.apply.v1", "fleet.v1.command.upload.pending.expire.apply", map[string]any{"id": "upload-apply-1", "effect_id": "effect-1"}},
		{"fleet.workflow.database.backup.v1", "fleet.v1.command.database.backup", map[string]any{"id": "db-backup-1", "database_id": "sessions", "object_key": "backups/sessions.db"}},
		{"fleet.workflow.database.backup.apply.v1", "fleet.v1.command.database.backup.apply", map[string]any{"id": "db-apply-1", "effect_id": "effect-1"}},
		{"fleet.workflow.archive.reconcile.v1", "fleet.v1.command.archive.reconcile", map[string]any{"id": "archive-reconcile-1", "server_id": "server-1"}},
		{"fleet.workflow.archive.reconcile.apply.v1", "fleet.v1.command.archive.reconcile.apply", map[string]any{"id": "archive-apply-1", "effect_id": "effect-1"}},
		{"fleet.workflow.backup.reconcile.v1", "fleet.v1.command.backup.reconcile", map[string]any{"id": "backup-reconcile-1", "server_id": "server-1"}},
		{"fleet.workflow.backup.reconcile.apply.v1", "fleet.v1.command.backup.reconcile.apply", map[string]any{"id": "backup-apply-1", "effect_id": "effect-1"}},
		{"fleet.workflow.world.orphan.sweep.v1", "fleet.v1.command.world.orphan.sweep", map[string]any{"id": "world-sweep-1", "object_prefix": "worlds/", "limit": int64(100)}},
		{"fleet.workflow.world.orphan.sweep.apply.v1", "fleet.v1.command.world.orphan.sweep.apply", map[string]any{"id": "world-sweep-apply-1", "effect_id": "effect-1"}},
		{"fleet.workflow.archive.stale.sweep.v1", "fleet.v1.command.archive.stale.sweep", map[string]any{"id": "archive-sweep-1", "object_prefix": "archives/", "limit": int64(100)}},
		{"fleet.workflow.archive.stale.sweep.apply.v1", "fleet.v1.command.archive.stale.sweep.apply", map[string]any{"id": "archive-sweep-apply-1", "effect_id": "effect-1"}},
	}
	for _, test := range tests {
		t.Run(test.event, func(t *testing.T) {
			calls := []string{}
			runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
				if target != "fleet" || function != test.provider {
					return nil, fmt.Errorf("unexpected archive call %s/%s", target, function)
				}
				var request map[string]any
				if err := msgpack.Unmarshal(payload, &request); err != nil {
					t.Fatal(err)
				}
				if request["id"] == nil {
					t.Fatalf("archive request lacks stable command id: %#v", request)
				}
				return fleetWire(map[string]any{"effects": []any{}})
			})
			defer runtime.Close()
			if _, err := runtime.Dispatch(workflow.DispatchRequest{
				Event: test.event, Payload: map[string]any{"request": test.request},
			}); err != nil {
				t.Fatalf("Dispatch(%s): %v", test.event, err)
			}
			assertFleetCalls(t, calls, "fleet/"+test.provider)
		})
	}
}
