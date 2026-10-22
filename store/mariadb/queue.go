package mariadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"kittiwake/store/queue"
)

const taskCols = "id, type, payload, run_after, attempts, last_error, created"

func scanTask(row interface{ Scan(...any) error }) (queue.Task, error) {
	var t queue.Task
	var payload []byte
	var lastErr sql.NullString
	if err := row.Scan(&t.ID, &t.Type, &payload, &t.RunAfter, &t.Attempts, &lastErr, &t.Created); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return queue.Task{}, queue.ErrNotFound
		}
		return queue.Task{}, err
	}
	t.Payload = json.RawMessage(payload)
	t.LastError = lastErr.String
	return t, nil
}

func (d *DB) Enqueue(ctx context.Context, typ string, payload any, runAfter time.Time) (string, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	now := d.Now().UTC()
	if runAfter.IsZero() {
		runAfter = now
	}
	id := uuid.NewString()
	_, err = d.exec(ctx, "INSERT INTO tasks (id, type, state, payload, run_after, attempts, created) VALUES (?, ?, 'pending', ?, ?, 0, ?)",
		id, typ, string(b), runAfter.UTC(), now)
	return id, err
}

// Claim leases up to max due tasks with SELECT ... FOR UPDATE SKIP LOCKED,
// so concurrent workers never take the same task. Expired leases of
// crashed workers are returned to pending first.
func (d *DB) Claim(ctx context.Context, typ, worker string, max int, lease time.Duration) ([]queue.Task, error) {
	now := d.Now().UTC()
	if _, err := d.exec(ctx, "UPDATE tasks SET state = 'pending', run_after = ?, lease_worker = NULL, lease_until = NULL WHERE type = ? AND state = 'leased' AND lease_until <= ?", now, typ, now); err != nil {
		return nil, err
	}
	var tasks []queue.Task
	err := d.tx(ctx, func(tx *sql.Tx) error {
		tasks = nil
		rows, err := tx.QueryContext(ctx, "SELECT "+taskCols+" FROM tasks WHERE type = ? AND state = 'pending' AND run_after <= ? ORDER BY run_after, id LIMIT ? FOR UPDATE SKIP LOCKED", typ, now, max)
		if err != nil {
			return err
		}
		for rows.Next() {
			t, err := scanTask(rows)
			if err != nil {
				rows.Close()
				return err
			}
			tasks = append(tasks, t)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		until := now.Add(lease)
		for i := range tasks {
			tasks[i].Attempts++
			tasks[i].LeaseKey = tasks[i].ID
			if _, err := tx.ExecContext(ctx, "UPDATE tasks SET state = 'leased', attempts = ?, lease_worker = ?, lease_until = ? WHERE id = ?", tasks[i].Attempts, worker, until, tasks[i].ID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (d *DB) Complete(ctx context.Context, t queue.Task) error {
	_, err := d.exec(ctx, "DELETE FROM tasks WHERE id = ? AND state = 'leased'", t.ID)
	return err
}

func (d *DB) Fail(ctx context.Context, t queue.Task, cause error) (bool, error) {
	now := d.Now().UTC()
	dead := t.Attempts >= d.MaxAttempts
	if dead {
		_, err := d.exec(ctx, "UPDATE tasks SET state = 'dead', last_error = ?, lease_worker = NULL, lease_until = NULL WHERE id = ?", cause.Error(), t.ID)
		return true, err
	}
	_, err := d.exec(ctx, "UPDATE tasks SET state = 'pending', last_error = ?, run_after = ?, lease_worker = NULL, lease_until = NULL WHERE id = ?", cause.Error(), now.Add(d.Backoff(t.Attempts)), t.ID)
	return false, err
}

// Extend renews the lease while the task is still leased; zero rows
// means the lease expired and Claim handed the task to another worker.
func (d *DB) Extend(ctx context.Context, t *queue.Task, lease time.Duration) error {
	now := d.Now().UTC()
	res, err := d.exec(ctx, "UPDATE tasks SET lease_until = ? WHERE id = ? AND state = 'leased' AND lease_until > ?", now.Add(lease), t.ID, now)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return queue.ErrLeaseLost
	}
	return nil
}

// Release returns a claimed task to pending, due now, without counting
// a failure.
func (d *DB) Release(ctx context.Context, t queue.Task) error {
	_, err := d.exec(ctx, "UPDATE tasks SET state = 'pending', run_after = ?, lease_worker = NULL, lease_until = NULL WHERE id = ? AND state = 'leased'", d.Now().UTC(), t.ID)
	return err
}

func (d *DB) Stats(ctx context.Context, typ string) (queue.Stats, error) {
	rows, err := d.r().QueryContext(ctx, "SELECT state, COUNT(*) FROM tasks WHERE type = ? GROUP BY state", typ)
	if err != nil {
		return queue.Stats{}, err
	}
	defer rows.Close()
	var st queue.Stats
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return st, err
		}
		switch state {
		case "pending":
			st.Pending = n
		case "leased":
			st.Leased = n
		case "dead":
			st.Dead = n
		}
	}
	return st, rows.Err()
}

func (d *DB) Dead(ctx context.Context, typ string) ([]queue.Task, error) {
	rows, err := d.r().QueryContext(ctx, "SELECT "+taskCols+" FROM tasks WHERE type = ? AND state = 'dead' ORDER BY created, id", typ)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []queue.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (d *DB) Retry(ctx context.Context, typ, id string) error {
	res, err := d.exec(ctx, "UPDATE tasks SET state = 'pending', attempts = 0, last_error = NULL, run_after = ? WHERE id = ? AND type = ? AND state = 'dead'", d.Now().UTC(), id, typ)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return queue.ErrNotFound
	}
	return nil
}

// ---- scheduler ----

func (d *DB) Lead(ctx context.Context, worker string, ttl time.Duration) (bool, error) {
	now := d.Now().UTC()
	var lead bool
	err := d.tx(ctx, func(tx *sql.Tx) error {
		var holder string
		var until time.Time
		err := tx.QueryRowContext(ctx, "SELECT holder, until FROM scheduler_lease WHERE id = 1 FOR UPDATE").Scan(&holder, &until)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			_, err = tx.ExecContext(ctx, "INSERT INTO scheduler_lease (id, holder, until) VALUES (1, ?, ?)", worker, now.Add(ttl))
			lead = err == nil
			return err
		case err != nil:
			return err
		case holder == worker || !until.After(now):
			_, err = tx.ExecContext(ctx, "UPDATE scheduler_lease SET holder = ?, until = ? WHERE id = 1", worker, now.Add(ttl))
			lead = err == nil
			return err
		}
		lead = false
		return nil
	})
	return lead, err
}

func (d *DB) Due(ctx context.Context, name string, every time.Duration) (bool, error) {
	now := d.Now().UTC()
	var due bool
	err := d.tx(ctx, func(tx *sql.Tx) error {
		var last time.Time
		err := tx.QueryRowContext(ctx, "SELECT last_run FROM scheduler WHERE name = ? FOR UPDATE", name).Scan(&last)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil && now.Sub(last) < every {
			due = false
			return nil
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO scheduler (name, last_run) VALUES (?, ?) ON DUPLICATE KEY UPDATE last_run = VALUES(last_run)", name, now)
		due = err == nil
		return err
	})
	return due, err
}

var _ queue.Store = (*DB)(nil)
