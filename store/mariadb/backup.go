package mariadb

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"

	"kittiwake/backup"
)

// The backup catalog: schedule state on the mailbox row, runs, blobs and
// jobs in their own tables. See backup.Catalog.

func (d *DB) Schedule(ctx context.Context, mb uuid.UUID, every time.Duration, due time.Time) error {
	var dueV any
	if every > 0 && !due.IsZero() {
		dueV = due.UTC()
	}
	_, err := d.exec(ctx, "UPDATE mailboxes SET backup_every=?, backup_due=? WHERE uuid=?", int64(every/time.Second), dueV, bin(mb))
	return err
}

func (d *DB) ScheduleMaintenance(ctx context.Context, mb uuid.UUID, due time.Time) error {
	_, err := d.exec(ctx, "UPDATE mailboxes SET backup_maintain_due=? WHERE uuid=?", nullTimeValue(due), bin(mb))
	return err
}

func (d *DB) BackupState(ctx context.Context, mb uuid.UUID) (backup.State, error) {
	s := backup.State{Mailbox: mb}
	var every sql.NullInt64
	var due, maint, at, verified sql.NullTime
	var run sql.NullString
	err := d.r(ctx).QueryRowContext(ctx, "SELECT backup_every, backup_due, backup_maintain_due, backup_run, backup_modseq, backup_at, backup_status, backup_error, backup_full, backup_verified, backup_verify_error FROM mailboxes WHERE uuid=?", bin(mb)).
		Scan(&every, &due, &maint, &run, &s.LastModSeq, &at, &s.Status, &s.Error, &s.ForceFull, &verified, &s.VerifyError)
	if errors.Is(err, sql.ErrNoRows) {
		return s, backup.ErrNotFound
	}
	if err != nil {
		return s, err
	}
	if every.Valid {
		s.Scheduled = true
		s.Every = time.Duration(every.Int64) * time.Second
	}
	s.LastRun = run.String
	for dst, src := range map[*time.Time]sql.NullTime{&s.Due: due, &s.MaintainDue: maint, &s.LastAt: at, &s.Verified: verified} {
		if src.Valid {
			*dst = src.Time.UTC()
		}
	}
	return s, nil
}

// catalogNamespaceID resolves the namespace of a catalog row: the name
// recorded on it, else the mailbox's, else the default.
func (d *DB) catalogNamespaceID(ctx context.Context, q querier, namespace string, mb uuid.UUID) (int64, error) {
	if strings.TrimSpace(namespace) != "" {
		return d.namespaceID(ctx, q, namespace)
	}
	var id int64
	err := q.QueryRowContext(ctx, "SELECT namespace_id FROM mailboxes WHERE uuid = ?", bin(mb)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 1, nil
	}
	return id, err
}

func (d *DB) FinishRun(ctx context.Context, run backup.Run, every time.Duration, due time.Time) error {
	packs, _ := json.Marshal(run.Packs)
	return d.tx(ctx, func(tx *sql.Tx) error {
		nsID, err := d.catalogNamespaceID(ctx, tx, run.Namespace, run.Mailbox)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO backup_runs (mailbox, namespace_id, id, started, finished, modseq, fuzzy, full, manifest_key, packs, bytes, messages, new_messages, expires) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE finished=VALUES(finished)",
			bin(run.Mailbox), nsID, run.ID, run.Started.UTC(), run.Finished.UTC(), run.ModSeq, run.Fuzzy, run.Full, run.ManifestKey, string(packs), run.Bytes, run.Messages, run.NewMessages, nullTimeValue(run.Expires)); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "UPDATE mailboxes SET backup_run=?, backup_modseq=?, backup_at=?, backup_status='ok', backup_error='', backup_full=0, backup_every=?, backup_due=? WHERE uuid=?",
			run.ID, run.ModSeq, run.Finished.UTC(), int64(every/time.Second), nullTimeValue(due), bin(run.Mailbox))
		return err
	})
}

func (d *DB) SkipRun(ctx context.Context, mb uuid.UUID, every time.Duration, due time.Time) error {
	_, err := d.exec(ctx, "UPDATE mailboxes SET backup_status='skipped', backup_error='', backup_every=?, backup_due=? WHERE uuid=?", int64(every/time.Second), nullTimeValue(due), bin(mb))
	return err
}

func (d *DB) FailRun(ctx context.Context, mb uuid.UUID, msg string, every time.Duration, retry time.Time) error {
	_, err := d.exec(ctx, "UPDATE mailboxes SET backup_status='failed', backup_error=?, backup_every=?, backup_due=? WHERE uuid=?", msg, int64(every/time.Second), nullTimeValue(retry), bin(mb))
	return err
}

func (d *DB) ForceFull(ctx context.Context, mb uuid.UUID, force bool) error {
	_, err := d.exec(ctx, "UPDATE mailboxes SET backup_full=? WHERE uuid=?", force, bin(mb))
	return err
}

func (d *DB) SetVerified(ctx context.Context, mb uuid.UUID, at time.Time, errText string) error {
	_, err := d.exec(ctx, "UPDATE mailboxes SET backup_verified=?, backup_verify_error=? WHERE uuid=?", at.UTC(), errText, bin(mb))
	return err
}

const runCols = "r.mailbox, n.name, r.id, r.started, r.finished, r.modseq, r.fuzzy, r.full, r.manifest_key, r.packs, r.bytes, r.messages, r.new_messages, r.expires"
const runFrom = " FROM backup_runs r JOIN namespaces n ON n.id = r.namespace_id "

func scanRun(row interface{ Scan(...any) error }) (backup.Run, error) {
	var r backup.Run
	var mb []byte
	var packs sql.NullString
	var expires sql.NullTime
	if err := row.Scan(&mb, &r.Namespace, &r.ID, &r.Started, &r.Finished, &r.ModSeq, &r.Fuzzy, &r.Full, &r.ManifestKey, &packs, &r.Bytes, &r.Messages, &r.NewMessages, &expires); err != nil {
		return r, err
	}
	r.Mailbox = toUUID(mb)
	r.Started, r.Finished = r.Started.UTC(), r.Finished.UTC()
	if packs.Valid {
		_ = json.Unmarshal([]byte(packs.String), &r.Packs)
	}
	if expires.Valid {
		r.Expires = expires.Time.UTC()
	}
	return r, nil
}

func (d *DB) GetRun(ctx context.Context, mb uuid.UUID, id string) (backup.Run, error) {
	r, err := scanRun(d.r(ctx).QueryRowContext(ctx, "SELECT "+runCols+runFrom+"WHERE r.mailbox=? AND r.id=?", bin(mb), id))
	if errors.Is(err, sql.ErrNoRows) {
		return r, backup.ErrNotFound
	}
	return r, err
}

func (d *DB) queryRuns(ctx context.Context, q string, args ...any) ([]backup.Run, error) {
	rows, err := d.r(ctx).QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backup.Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (d *DB) ListRuns(ctx context.Context, mb uuid.UUID, max int) ([]backup.Run, error) {
	if max <= 0 {
		max = 100000
	}
	return d.queryRuns(ctx, "SELECT "+runCols+runFrom+"WHERE r.mailbox=? ORDER BY r.id DESC LIMIT ?", bin(mb), max)
}

func (d *DB) DeleteRun(ctx context.Context, mb uuid.UUID, id string) error {
	_, err := d.exec(ctx, "DELETE FROM backup_runs WHERE mailbox=? AND id=?", bin(mb), id)
	return err
}

func (d *DB) ExpiredRuns(ctx context.Context, namespace string, before time.Time, max int) ([]backup.Run, error) {
	if max <= 0 {
		max = 1000
	}
	if namespace == "" {
		return d.queryRuns(ctx, "SELECT "+runCols+runFrom+"WHERE r.expires IS NOT NULL AND r.expires<=? ORDER BY r.expires LIMIT ?", before.UTC(), max)
	}
	nsID, err := d.namespaceID(ctx, d.r(ctx), namespace)
	if err != nil {
		return nil, err
	}
	return d.queryRuns(ctx, "SELECT "+runCols+runFrom+"WHERE r.namespace_id=? AND r.expires IS NOT NULL AND r.expires<=? ORDER BY r.expires LIMIT ?", nsID, before.UTC(), max)
}

func (d *DB) RunMailboxes(ctx context.Context, fn func(uuid.UUID) error) error {
	after := []byte{} // BINARY compares bytewise; NULL would match nothing
	for {
		rows, err := d.r(ctx).QueryContext(ctx, "SELECT DISTINCT mailbox FROM backup_runs WHERE mailbox>? ORDER BY mailbox LIMIT 512", after)
		if err != nil {
			return err
		}
		var batch [][]byte
		for rows.Next() {
			var mb []byte
			if err := rows.Scan(&mb); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, mb)
		}
		rows.Close()
		if len(batch) == 0 {
			return nil
		}
		for _, mb := range batch {
			if err := fn(toUUID(mb)); err != nil {
				return err
			}
		}
		after = batch[len(batch)-1]
	}
}

func (d *DB) HaveBlobs(ctx context.Context, names []string) (map[string]backup.Blob, error) {
	out := map[string]backup.Blob{}
	if len(names) == 0 {
		return out, nil
	}
	args := make([]any, len(names))
	for i, n := range names {
		args[i] = n
	}
	rows, err := d.r(ctx).QueryContext(ctx, "SELECT b.name, n.name, b.size, b.sealed, b.created FROM backup_blobs b JOIN namespaces n ON n.id = b.namespace_id WHERE b.name IN (?"+strings.Repeat(",?", len(names)-1)+")", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var b backup.Blob
		if err := rows.Scan(&b.Name, &b.Namespace, &b.Size, &b.Sealed, &b.Created); err != nil {
			return nil, err
		}
		out[b.Name] = b
	}
	return out, rows.Err()
}

func (d *DB) AddBlobs(ctx context.Context, blobs []backup.Blob) error {
	for _, b := range blobs {
		nsID, err := d.catalogNamespaceID(ctx, d.sql, b.Namespace, uuid.Nil)
		if err != nil {
			return err
		}
		if _, err := d.exec(ctx, "INSERT IGNORE INTO backup_blobs (name, namespace_id, size, sealed, created) VALUES (?,?,?,?,?)", b.Name, nsID, b.Size, b.Sealed, b.Created.UTC()); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) AddBlobRefs(ctx context.Context, mb uuid.UUID, names []string) error {
	for _, n := range names {
		if _, err := d.exec(ctx, "INSERT IGNORE INTO backup_blob_refs (name, mailbox) VALUES (?,?)", n, bin(mb)); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) DropBlobRefs(ctx context.Context, mb uuid.UUID) error {
	_, err := d.exec(ctx, "DELETE FROM backup_blob_refs WHERE mailbox=?", bin(mb))
	return err
}

func (d *DB) UnreferencedBlobs(ctx context.Context, namespace string, before time.Time, max int) ([]backup.Blob, error) {
	if max <= 0 {
		max = 1000
	}
	q := "SELECT b.name, n.name, b.size, b.sealed, b.created FROM backup_blobs b JOIN namespaces n ON n.id = b.namespace_id LEFT JOIN backup_blob_refs r ON r.name=b.name WHERE r.name IS NULL AND b.created<=?"
	args := []any{before.UTC()}
	if namespace != "" {
		nsID, err := d.namespaceID(ctx, d.r(ctx), namespace)
		if err != nil {
			return nil, err
		}
		q += " AND b.namespace_id=?"
		args = append(args, nsID)
	}
	rows, err := d.r(ctx).QueryContext(ctx, q+" LIMIT ?", append(args, max)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backup.Blob
	for rows.Next() {
		var b backup.Blob
		if err := rows.Scan(&b.Name, &b.Namespace, &b.Size, &b.Sealed, &b.Created); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (d *DB) DeleteBlob(ctx context.Context, name string) error {
	_, err := d.exec(ctx, "DELETE FROM backup_blobs WHERE name=?", name)
	return err
}

const jobCols = "j.id, j.kind, j.mailbox, n.name, j.run_id, j.target, j.message, j.object_key, j.url, j.detail, j.bytes, j.messages, j.state, j.error, j.requested_by, j.created, j.finished, j.expires"
const jobFrom = " FROM backup_jobs j JOIN namespaces n ON n.id = j.namespace_id "

func scanJob(row interface{ Scan(...any) error }) (backup.Job, error) {
	var j backup.Job
	var mb, target, msg []byte
	var detail sql.NullString
	var finished, expires sql.NullTime
	if err := row.Scan(&j.ID, &j.Kind, &mb, &j.Namespace, &j.RunID, &target, &msg, &j.ObjectKey, &j.URL, &detail, &j.Bytes, &j.Messages, &j.State, &j.Error, &j.Requested, &j.Created, &finished, &expires); err != nil {
		return j, err
	}
	j.Mailbox = toUUID(mb)
	if target != nil {
		j.Target = toUUID(target)
	}
	if msg != nil {
		j.MessageID = toUUID(msg)
	}
	if detail.Valid && detail.String != "" {
		j.Detail = json.RawMessage(detail.String)
	}
	j.Created = j.Created.UTC()
	if finished.Valid {
		j.Finished = finished.Time.UTC()
	}
	if expires.Valid {
		j.Expires = expires.Time.UTC()
	}
	return j, nil
}

func (d *DB) PutJob(ctx context.Context, j backup.Job) error {
	var detail any
	if len(j.Detail) > 0 {
		detail = string(j.Detail)
	}
	nsID, err := d.catalogNamespaceID(ctx, d.sql, j.Namespace, j.Mailbox)
	if err != nil {
		return err
	}
	_, err = d.exec(ctx, "INSERT INTO backup_jobs (id, kind, mailbox, namespace_id, run_id, target, message, object_key, url, detail, bytes, messages, state, error, requested_by, created, finished, expires) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE target=VALUES(target), object_key=VALUES(object_key), url=VALUES(url), detail=VALUES(detail), bytes=VALUES(bytes), messages=VALUES(messages), state=VALUES(state), error=VALUES(error), finished=VALUES(finished), expires=VALUES(expires)",
		j.ID, j.Kind, bin(j.Mailbox), nsID, j.RunID, nullBin(j.Target), nullBin(j.MessageID), j.ObjectKey, j.URL, detail, j.Bytes, j.Messages, j.State, j.Error, j.Requested, j.Created.UTC(), nullTimeValue(j.Finished), nullTimeValue(j.Expires))
	return err
}

func (d *DB) GetJob(ctx context.Context, id string) (backup.Job, error) {
	j, err := scanJob(d.r(ctx).QueryRowContext(ctx, "SELECT "+jobCols+jobFrom+"WHERE j.id=?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return j, backup.ErrNotFound
	}
	return j, err
}

func (d *DB) queryJobs(ctx context.Context, q string, args ...any) ([]backup.Job, error) {
	rows, err := d.r(ctx).QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backup.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (d *DB) ListJobs(ctx context.Context, mb uuid.UUID, kind string, max int) ([]backup.Job, error) {
	if max <= 0 {
		max = 100
	}
	q := "SELECT " + jobCols + jobFrom + "WHERE 1=1"
	var args []any
	if mb != uuid.Nil {
		q += " AND j.mailbox=?"
		args = append(args, bin(mb))
	}
	if kind != "" {
		q += " AND j.kind=?"
		args = append(args, kind)
	}
	q += " ORDER BY j.created DESC LIMIT ?"
	args = append(args, max)
	return d.queryJobs(ctx, q, args...)
}

func (d *DB) ExpiredJobs(ctx context.Context, namespace string, before time.Time, max int) ([]backup.Job, error) {
	if max <= 0 {
		max = 1000
	}
	if namespace == "" {
		return d.queryJobs(ctx, "SELECT "+jobCols+jobFrom+"WHERE j.expires IS NOT NULL AND j.expires<=? ORDER BY j.expires LIMIT ?", before.UTC(), max)
	}
	nsID, err := d.namespaceID(ctx, d.r(ctx), namespace)
	if err != nil {
		return nil, err
	}
	return d.queryJobs(ctx, "SELECT "+jobCols+jobFrom+"WHERE j.namespace_id=? AND j.expires IS NOT NULL AND j.expires<=? ORDER BY j.expires LIMIT ?", nsID, before.UTC(), max)
}

func (d *DB) DeleteJob(ctx context.Context, id string) error {
	_, err := d.exec(ctx, "DELETE FROM backup_jobs WHERE id=?", id)
	return err
}

// EnqueueDue inserts backup and maintenance tasks for due mailboxes in
// the same transaction that moves their due time, with the periodic key
// coalescing a pending or leased task for the same mailbox. The enqueue
// callback is not used: the task rows are written here.
func (d *DB) EnqueueDue(ctx context.Context, namespace string, limit int, _ func(context.Context, string, uuid.UUID) error) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	now := d.Now().UTC()
	total := 0
	nsID, err := d.namespaceID(ctx, d.sql, namespace)
	if err != nil {
		return 0, err
	}
	for _, kind := range []struct {
		typ, col, bump string
	}{
		{backup.TaskBackup, "backup_due", "backup_due=DATE_ADD(?, INTERVAL backup_every SECOND)"},
		{backup.TaskMaintain, "backup_maintain_due", "backup_maintain_due=DATE_ADD(?, INTERVAL 1 DAY)"},
	} {
		err := d.backgroundTx(ctx, func(tx *sql.Tx) error {
			rows, err := tx.QueryContext(ctx, "SELECT id, uuid FROM mailboxes WHERE namespace_id=? AND "+kind.col+"<=? AND backup_every>0 ORDER BY "+kind.col+", id LIMIT ? FOR UPDATE SKIP LOCKED", nsID, now, limit)
			if err != nil {
				return err
			}
			type due struct {
				id int64
				mb uuid.UUID
			}
			var batch []due
			for rows.Next() {
				var r due
				var raw []byte
				if err := rows.Scan(&r.id, &raw); err != nil {
					rows.Close()
					return err
				}
				r.mb = toUUID(raw)
				batch = append(batch, r)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			for _, r := range batch {
				key := sha256.Sum256([]byte(kind.typ + "\x00" + r.mb.String()))
				payload, _ := json.Marshal(backup.BackupTask{Mailbox: r.mb})
				if _, err := tx.ExecContext(ctx, "INSERT INTO tasks (id,type,namespace_id,state,payload,run_after,attempts,created,periodic_key) VALUES (?,?,?,'pending',?,?,0,?,?) ON DUPLICATE KEY UPDATE id=id", uuid.NewString(), kind.typ, nsID, string(payload), now, now, key[:]); err != nil {
					return err
				}
				// The task sets the real next due time when it finishes; this
				// bump only keeps the row out of the due index meanwhile and
				// brings it back if the task is lost.
				if _, err := tx.ExecContext(ctx, "UPDATE mailboxes SET "+kind.bump+" WHERE id=?", now, r.id); err != nil {
					return err
				}
				total++
			}
			return nil
		})
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// Reconcile gives mailboxes without backup state their first schedule,
// walking the registry in bounded, resumable steps.
func (d *DB) Reconcile(ctx context.Context, namespace string, limit int, every time.Duration, defaults func(context.Context, uuid.UUID) time.Duration) error {
	if limit <= 0 {
		return nil
	}
	now := d.Now().UTC()
	return d.backgroundTx(ctx, func(tx *sql.Tx) error {
		nsID, err := d.namespaceID(ctx, tx, namespace)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT IGNORE INTO maintenance_scans (name, namespace_id) VALUES ('backup', ?)", nsID); err != nil {
			return err
		}
		var after, ceiling int64
		var completed sql.NullTime
		if err := tx.QueryRowContext(ctx, "SELECT after_id,ceiling_id,completed_at FROM maintenance_scans WHERE name='backup' AND namespace_id=? FOR UPDATE", nsID).Scan(&after, &ceiling, &completed); err != nil {
			return err
		}
		if ceiling == 0 {
			if completed.Valid && now.Sub(completed.Time) < every/24 {
				return nil // a full pass per hour at the default interval
			}
			if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(id),0) FROM mailboxes WHERE namespace_id=?", nsID).Scan(&ceiling); err != nil {
				return err
			}
		}
		rows, err := tx.QueryContext(ctx, "SELECT id, uuid, backup_every FROM mailboxes WHERE namespace_id=? AND id>? AND id<=? ORDER BY id LIMIT ?", nsID, after, ceiling, limit)
		if err != nil {
			return err
		}
		type row struct {
			id    int64
			mb    uuid.UUID
			unset bool
		}
		var batch []row
		for rows.Next() {
			var r row
			var raw []byte
			var ev sql.NullInt64
			if err := rows.Scan(&r.id, &raw, &ev); err != nil {
				rows.Close()
				return err
			}
			r.mb, r.unset = toUUID(raw), !ev.Valid
			batch = append(batch, r)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, r := range batch {
			if !r.unset {
				continue
			}
			d := defaults(ctx, r.mb)
			var due any
			if d > 0 {
				due = backup.FirstDue(now, d).UTC()
			}
			if _, err := tx.ExecContext(ctx, "UPDATE mailboxes SET backup_every=?, backup_due=?, backup_maintain_due=? WHERE id=? AND backup_every IS NULL", int64(d/time.Second), due, backup.FirstDue(now, 7*24*time.Hour).UTC(), r.id); err != nil {
				return err
			}
		}
		if len(batch) > 0 {
			after = batch[len(batch)-1].id
		}
		if len(batch) < limit || after >= ceiling {
			_, err = tx.ExecContext(ctx, "UPDATE maintenance_scans SET after_id=0,ceiling_id=0,completed_at=? WHERE name='backup' AND namespace_id=?", now, nsID)
		} else {
			_, err = tx.ExecContext(ctx, "UPDATE maintenance_scans SET after_id=?,ceiling_id=? WHERE name='backup' AND namespace_id=?", after, ceiling, nsID)
		}
		return err
	})
}

// LeadBackups takes the namespace's backup scheduler lease (lease kind 2;
// the workers' scheduler holds kind 1).
func (d *DB) LeadBackups(ctx context.Context, namespace, holder string, ttl time.Duration) (bool, error) {
	return d.lead(ctx, 2, namespace, holder, ttl)
}

func (d *DB) Summary(ctx context.Context) (backup.Summary, error) {
	now := d.Now().UTC()
	var s backup.Summary
	var oldest sql.NullTime
	var rate sql.NullFloat64
	err := d.r(ctx).QueryRowContext(ctx, `SELECT COUNT(*),
		COALESCE(SUM(backup_every>0),0), COALESCE(SUM(backup_every=0),0),
		COALESCE(SUM(backup_every>0 AND backup_due<?),0), COALESCE(SUM(backup_status='failed'),0), COALESCE(SUM(backup_verify_error<>''),0),
		MIN(CASE WHEN backup_every>0 AND backup_due<? THEN backup_due END), SUM(CASE WHEN backup_every>0 THEN 3600/backup_every END)
		FROM mailboxes`, now, now).Scan(&s.Mailboxes, &s.Scheduled, &s.Disabled, &s.Overdue, &s.Failed, &s.VerifyFailed, &oldest, &rate)
	if err != nil {
		return s, err
	}
	if oldest.Valid {
		s.OldestDue = oldest.Time.UTC()
	}
	s.Rate = rate.Float64
	if err := d.r(ctx).QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(SUM(bytes),0) FROM backup_runs").Scan(&s.Runs, &s.Bytes); err != nil {
		return s, err
	}
	if err := d.r(ctx).QueryRowContext(ctx, "SELECT COUNT(*) FROM backup_blobs").Scan(&s.Blobs); err != nil {
		return s, err
	}
	err = d.r(ctx).QueryRowContext(ctx, "SELECT COUNT(*) FROM backup_jobs WHERE state IN ('pending','running')").Scan(&s.Jobs)
	return s, err
}

var _ backup.Catalog = (*DB)(nil)
