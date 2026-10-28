package mariadb

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

func (d *DB) SchedulePurge(ctx context.Context, mb uuid.UUID, at time.Time) error {
	if at.IsZero() {
		return nil
	}
	_, err := d.sql.ExecContext(ctx, "UPDATE mailboxes SET purge_after=? WHERE uuid=? AND (purge_after IS NULL OR purge_after>?)", at.UTC(), mb[:], at.UTC())
	return err
}

// The registry row and queue insertion are changed in one transaction. A signal
// arriving while a task is leased remains in the registry for a successor; it
// must never be discarded as a duplicate of work already in flight.
func (d *DB) EnqueueDuePurges(ctx context.Context, limit int) (n int, err error) {
	if limit <= 0 {
		return 0, nil
	}
	now := d.Now().UTC()
	err = d.backgroundTx(ctx, func(tx *sql.Tx) error {
		n = 0
		rows, err := tx.QueryContext(ctx, "SELECT id,uuid FROM mailboxes WHERE purge_after<=? ORDER BY purge_after,id LIMIT ? FOR UPDATE SKIP LOCKED", now, limit)
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
			key := sha256.Sum256([]byte("purge-trash\x00" + r.mb.String()))
			payload, _ := json.Marshal(map[string]uuid.UUID{"mailbox": r.mb})
			_, err := tx.ExecContext(ctx, "INSERT INTO tasks (id,type,state,payload,run_after,attempts,created,periodic_key) VALUES (?,'purge-trash','pending',?,?,0,?,?) ON DUPLICATE KEY UPDATE id=id", uuid.NewString(), string(payload), now, now, key[:])
			if err != nil {
				return err
			}
			var state string
			if err := tx.QueryRowContext(ctx, "SELECT state FROM tasks WHERE periodic_key=? FOR UPDATE", key[:]).Scan(&state); err != nil {
				return err
			}
			if state == "leased" {
				// Move it out of the front of the due index while the old task runs.
				_, err = tx.ExecContext(ctx, "UPDATE mailboxes SET purge_after=? WHERE id=?", now.Add(time.Minute), r.id)
			} else {
				_, err = tx.ExecContext(ctx, "UPDATE mailboxes SET purge_after=NULL WHERE id=?", r.id)
			}
			if err != nil {
				return err
			}
			n++
		}
		return nil
	})
	return
}

// A bounded keyset scan repairs missing signals across the Ceph/SQL boundary.
// The cursor and updates commit together. A scheduler restart resumes the scan;
// concurrent mailbox creation cannot extend its fixed high watermark forever.
func (d *DB) ReconcilePurgeSchedule(ctx context.Context, limit int, every time.Duration) error {
	if limit <= 0 {
		return nil
	}
	now := d.Now().UTC()
	return d.backgroundTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "INSERT IGNORE INTO maintenance_scans (name) VALUES ('purge')"); err != nil {
			return err
		}
		var after, ceiling int64
		var completed sql.NullTime
		if err := tx.QueryRowContext(ctx, "SELECT after_id,ceiling_id,completed_at FROM maintenance_scans WHERE name='purge' FOR UPDATE").Scan(&after, &ceiling, &completed); err != nil {
			return err
		}
		if ceiling == 0 {
			if completed.Valid && now.Sub(completed.Time) < every {
				return nil
			}
			if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(id),0) FROM mailboxes").Scan(&ceiling); err != nil {
				return err
			}
		}
		rows, err := tx.QueryContext(ctx, "SELECT id FROM mailboxes WHERE id>? AND id<=? ORDER BY id LIMIT ?", after, ceiling, limit)
		if err != nil {
			return err
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(ids) > 0 {
			last := ids[len(ids)-1]
			if _, err := tx.ExecContext(ctx, "UPDATE mailboxes SET purge_after=? WHERE id>? AND id<=? AND purge_after IS NULL", now, after, last); err != nil {
				return err
			}
			after = last
		}
		if len(ids) < limit || after >= ceiling {
			_, err = tx.ExecContext(ctx, "UPDATE maintenance_scans SET after_id=0,ceiling_id=0,completed_at=? WHERE name='purge'", now)
		} else {
			_, err = tx.ExecContext(ctx, "UPDATE maintenance_scans SET after_id=?,ceiling_id=? WHERE name='purge'", after, ceiling)
		}
		return err
	})
}
