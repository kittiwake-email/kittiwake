package mariadb_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"kittiwake/backup"
	"kittiwake/store/identity"
)

func TestBackupCatalog(t *testing.T) {
	db := open(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	db.Now = func() time.Time { return now }
	ids := identity.NewWith(db)
	ids.Now = func() time.Time { return now }
	mb, err := ids.CreateMailbox(ctx, identity.Mailbox{Backup: identity.BackupSettings{Interval: 2 * time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := ids.GetMailbox(ctx, mb.ID)
	if got.Value.Backup.Interval != 2*time.Hour || got.Value.Backup.Disabled {
		t.Fatalf("settings round trip: %+v", got.Value.Backup)
	}
	_, _ = ids.UpdateMailbox(ctx, mb.ID, func(m *identity.Mailbox) error { m.Backup.Disabled = true; m.Backup.Retention = time.Hour; return nil })
	got, _ = ids.GetMailbox(ctx, mb.ID)
	if !got.Value.Backup.Disabled || got.Value.Backup.Retention != time.Hour || got.Value.Backup.Interval != 2*time.Hour {
		t.Fatalf("settings update: %+v", got.Value.Backup)
	}

	// Unscheduled until reconciled; reconcile asks the policy for the interval.
	st, err := db.BackupState(ctx, mb.ID)
	if err != nil || st.Scheduled {
		t.Fatalf("state: %+v %v", st, err)
	}
	if _, err := db.BackupState(ctx, uuid.New()); !errors.Is(err, backup.ErrNotFound) {
		t.Fatalf("unknown: %v", err)
	}
	if err := db.Reconcile(ctx, 100, 24*time.Hour, func(context.Context, uuid.UUID) time.Duration { return time.Hour }); err != nil {
		t.Fatal(err)
	}
	st, _ = db.BackupState(ctx, mb.ID)
	if !st.Scheduled || st.Every != time.Hour || st.Due.Before(now) || st.Due.After(now.Add(time.Hour)) || st.MaintainDue.IsZero() {
		t.Fatalf("reconciled: %+v", st)
	}
	// Nothing due yet; then due: a task is inserted once (coalesced) and the
	// due time bumped.
	if n, err := db.EnqueueDue(ctx, 10, nil); err != nil || n != 0 {
		t.Fatalf("not due: %d %v", n, err)
	}
	_ = db.Schedule(ctx, mb.ID, time.Hour, now.Add(-time.Minute))
	if n, err := db.EnqueueDue(ctx, 10, nil); err != nil || n != 1 {
		t.Fatalf("due: %d %v", n, err)
	}
	st, _ = db.BackupState(ctx, mb.ID)
	if !st.Due.Equal(now.Add(time.Hour)) {
		t.Fatalf("bumped due: %+v", st)
	}
	_ = db.Schedule(ctx, mb.ID, time.Hour, now.Add(-time.Minute))
	if n, _ := db.EnqueueDue(ctx, 10, nil); n != 1 {
		t.Fatal("second signal recorded")
	}
	tasks, err := db.Claim(ctx, backup.TaskBackup, "b1", 10, time.Hour)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("one coalesced task: %d %v", len(tasks), err)
	}
	// Finish a run.
	run := backup.Run{ID: "20261028T120000Z-aaaa", Mailbox: mb.ID, Started: now, Finished: now, ModSeq: 7, Full: true, ManifestKey: "kw/mb/x/1.manifest", Packs: []string{"kw/mb/x/1.0.pack"}, Bytes: 10, Messages: 3, NewMessages: 3, Expires: now.Add(48 * time.Hour)}
	if err := db.FinishRun(ctx, run, time.Hour, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	st, _ = db.BackupState(ctx, mb.ID)
	if st.LastRun != run.ID || st.LastModSeq != 7 || st.Status != "ok" || !st.LastAt.Equal(now) {
		t.Fatalf("finished: %+v", st)
	}
	r, err := db.GetRun(ctx, mb.ID, run.ID)
	if err != nil || r.Packs[0] != "kw/mb/x/1.0.pack" || !r.Expires.Equal(run.Expires) || !r.Full {
		t.Fatalf("run: %+v %v", r, err)
	}
	run2 := run
	run2.ID, run2.Expires = "20261028T130000Z-bbbb", time.Time{}
	_ = db.FinishRun(ctx, run2, time.Hour, now.Add(time.Hour))
	runs, _ := db.ListRuns(ctx, mb.ID, 0)
	if len(runs) != 2 || runs[0].ID != run2.ID {
		t.Fatalf("list: %+v", runs)
	}
	exp, _ := db.ExpiredRuns(ctx, now.Add(49*time.Hour), 10)
	if len(exp) != 1 || exp[0].ID != run.ID {
		t.Fatalf("expired: %+v", exp)
	}
	var seen []uuid.UUID
	_ = db.RunMailboxes(ctx, func(id uuid.UUID) error { seen = append(seen, id); return nil })
	if len(seen) != 1 || seen[0] != mb.ID {
		t.Fatalf("run mailboxes: %v", seen)
	}
	_ = db.FailRun(ctx, mb.ID, "boom", time.Hour, now.Add(time.Minute))
	_ = db.ForceFull(ctx, mb.ID, true)
	_ = db.SetVerified(ctx, mb.ID, now, "1 missing")
	st, _ = db.BackupState(ctx, mb.ID)
	if st.Status != "failed" || st.Error != "boom" || !st.ForceFull || st.VerifyError != "1 missing" || !st.Verified.Equal(now) {
		t.Fatalf("failed state: %+v", st)
	}
	_ = db.SkipRun(ctx, mb.ID, time.Hour, now.Add(time.Hour))
	if st, _ = db.BackupState(ctx, mb.ID); st.Status != "skipped" {
		t.Fatalf("skipped: %+v", st)
	}
	_ = db.DeleteRun(ctx, mb.ID, run.ID)
	if _, err := db.GetRun(ctx, mb.ID, run.ID); !errors.Is(err, backup.ErrNotFound) {
		t.Fatal("deleted run")
	}

	// Blobs and references.
	blobs := []backup.Blob{{Name: "a", Size: 1, Sealed: 2, Created: now.Add(-72 * time.Hour)}, {Name: "b", Size: 1, Sealed: 2, Created: now}}
	_ = db.AddBlobs(ctx, blobs)
	_ = db.AddBlobs(ctx, blobs[:1]) // idempotent
	have, _ := db.HaveBlobs(ctx, []string{"a", "zz"})
	if len(have) != 1 || have["a"].Sealed != 2 {
		t.Fatalf("have: %+v", have)
	}
	_ = db.AddBlobRefs(ctx, mb.ID, []string{"a"})
	_ = db.AddBlobRefs(ctx, mb.ID, []string{"a"})
	un, _ := db.UnreferencedBlobs(ctx, now.Add(-time.Hour), 10)
	if len(un) != 0 {
		t.Fatalf("b is young, a referenced: %+v", un)
	}
	_ = db.DropBlobRefs(ctx, mb.ID)
	un, _ = db.UnreferencedBlobs(ctx, now.Add(-time.Hour), 10)
	if len(un) != 1 || un[0].Name != "a" {
		t.Fatalf("unreferenced: %+v", un)
	}
	_ = db.DeleteBlob(ctx, "a")
	if have, _ := db.HaveBlobs(ctx, []string{"a"}); len(have) != 0 {
		t.Fatal("deleted blob")
	}

	// Jobs.
	j := backup.Job{ID: uuid.NewString(), Kind: backup.JobExport, Mailbox: mb.ID, RunID: run2.ID, State: backup.JobPending, Requested: "root", Created: now}
	if err := db.PutJob(ctx, j); err != nil {
		t.Fatal(err)
	}
	j.State, j.ObjectKey, j.Bytes, j.Finished, j.Expires, j.Target = backup.JobDone, "kw/export/x", 5, now, now.Add(time.Hour), uuid.New()
	if err := db.PutJob(ctx, j); err != nil {
		t.Fatal(err)
	}
	got2, err := db.GetJob(ctx, j.ID)
	if err != nil || got2.State != backup.JobDone || got2.ObjectKey != "kw/export/x" || got2.Target != j.Target || !got2.Expires.Equal(j.Expires) {
		t.Fatalf("job: %+v %v", got2, err)
	}
	if jobs, _ := db.ListJobs(ctx, mb.ID, backup.JobExport, 10); len(jobs) != 1 {
		t.Fatalf("list jobs: %+v", jobs)
	}
	if jobs, _ := db.ListJobs(ctx, uuid.Nil, backup.JobRestore, 10); len(jobs) != 0 {
		t.Fatal("kind filter")
	}
	if ej, _ := db.ExpiredJobs(ctx, now.Add(2*time.Hour), 10); len(ej) != 1 {
		t.Fatalf("expired jobs: %+v", ej)
	}
	_ = db.DeleteJob(ctx, j.ID)
	if _, err := db.GetJob(ctx, j.ID); !errors.Is(err, backup.ErrNotFound) {
		t.Fatal("deleted job")
	}

	// Lease and summary.
	if lead, err := db.LeadBackups(ctx, "n1", time.Minute); err != nil || !lead {
		t.Fatalf("lead: %v %v", lead, err)
	}
	if lead, _ := db.LeadBackups(ctx, "n2", time.Minute); lead {
		t.Fatal("second leader")
	}
	if lead, _ := db.Lead(ctx, "worker", time.Minute); !lead {
		t.Fatal("the worker scheduler lease is separate")
	}
	sum, err := db.Summary(ctx)
	if err != nil || sum.Mailboxes != 1 || sum.Scheduled != 1 || sum.Runs != 1 || sum.Blobs != 1 || sum.Rate != 1 {
		t.Fatalf("summary: %+v %v", sum, err)
	}
}
