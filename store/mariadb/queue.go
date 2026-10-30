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

// ServeNamespaces restricts Claim to tasks of these namespaces (queue.Namespaced).
func (d *DB) ServeNamespaces(namespaces []string) {
	d.served = append([]string(nil), namespaces...)
}

// taskNamespaceID resolves the namespace of a task from its payload: an
// explicit "namespace" field, else the namespace of its "mailbox", else
// the default namespace.
func (d *DB) taskNamespaceID(ctx context.Context, q querier, payload []byte) int64 {
	var p struct {
		Namespace string    `json:"namespace"`
		Mailbox   uuid.UUID `json:"mailbox"`
	}
	_ = json.Unmarshal(payload, &p)
	if p.Namespace != "" {
		if id, err := d.namespaceID(ctx, q, p.Namespace); err == nil {
			return id
		}
	}
	if p.Mailbox != uuid.Nil {
		var id int64
		if err := q.QueryRowContext(ctx, "SELECT namespace_id FROM mailboxes WHERE uuid = ?", bin(p.Mailbox)).Scan(&id); err == nil {
			return id
		}
	}
	return 1
}

// servedFilter returns a SQL fragment (starting with " AND ") and its
// arguments restricting tasks to the served namespaces; empty when all.
func (d *DB) servedFilter() (string, []any) {
	if len(d.served) == 0 {
		return "", nil
	}
	args := make([]any, len(d.served))
	for i, ns := range d.served {
		args[i] = ns
	}
	return " AND namespace_id IN (SELECT id FROM namespaces WHERE name IN (?" + strings.Repeat(",?", len(d.served)-1) + "))", args
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
	nsID := d.taskNamespaceID(ctx, d.sql, b)
	_, err = d.sql.ExecContext(ctx, "INSERT INTO tasks (id, type, namespace_id, state, payload, run_after, attempts, created) VALUES (?, ?, ?, 'pending', ?, ?, 0, ?)",
		id, typ, nsID, string(b), runAfter.UTC(), now)
	return id, err
}

// Claim leases up to max due tasks with SELECT ... FOR UPDATE SKIP LOCKED,
// so concurrent workers never take the same task. Expired leases of
// crashed workers are returned to pending first.
func (d *DB) Claim(ctx context.Context, typ, worker string, max int, lease time.Duration) ([]queue.Task, error) {
	now := d.Now().UTC()
	if _, err := d.sql.ExecContext(ctx, "UPDATE tasks SET state = 'pending', run_after = ?, lease_worker = NULL, lease_until = NULL WHERE type = ? AND state = 'leased' AND lease_until <= ?", now, typ, now); err != nil {
		return nil, err
	}
	var tasks []queue.Task
	filter, fargs := d.servedFilter()
	err := d.backgroundTx(ctx, func(tx *sql.Tx) error {
		tasks = nil
		args := append([]any{typ, now}, fargs...)
		rows, err := tx.QueryContext(ctx, "SELECT "+taskCols+" FROM tasks WHERE type = ? AND state = 'pending' AND run_after <= ?"+filter+" ORDER BY run_after, id LIMIT ? FOR UPDATE SKIP LOCKED", append(args, max)...)
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
			tasks[i].LeaseKey = uuid.NewString()
			if _, err := tx.ExecContext(ctx, "UPDATE tasks SET state = 'leased', attempts = ?, lease_worker = ?, lease_until = ?, lease_token = ? WHERE id = ?", tasks[i].Attempts, worker, until, tasks[i].LeaseKey, tasks[i].ID); err != nil {
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

// leasedExec fences every state transition to this claim, including after takeover.
func (d *DB) leasedExec(ctx context.Context, t queue.Task, statement string, args ...any) error {
	args = append(args, t.ID, t.LeaseKey, d.Now().UTC())
	res, err := d.sql.ExecContext(ctx, statement+" WHERE id = ? AND state = 'leased' AND lease_token = ? AND lease_until > ?", args...)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return queue.ErrLeaseLost
	}
	return nil
}

func (d *DB) Complete(ctx context.Context, t queue.Task) error {
	return d.leasedExec(ctx, t, "DELETE FROM tasks")
}
func (d *DB) Fail(ctx context.Context, t queue.Task, cause error) (bool, error) {
	dead := t.Attempts >= d.MaxAttempts
	if dead {
		return true, d.leasedExec(ctx, t, "UPDATE tasks SET state = 'dead', periodic_key = NULL, last_error = ?, lease_worker = NULL, lease_until = NULL, lease_token = NULL", cause.Error())
	}
	return false, d.leasedExec(ctx, t, "UPDATE tasks SET state = 'pending', last_error = ?, run_after = ?, lease_worker = NULL, lease_until = NULL, lease_token = NULL", cause.Error(), d.Now().UTC().Add(d.Backoff(t.Attempts)))
}
func (d *DB) Extend(ctx context.Context, t *queue.Task, lease time.Duration) error {
	// Rotate the token too: even a renewal with the same expiry changes a row.
	next := uuid.NewString()
	err := d.leasedExec(ctx, *t, "UPDATE tasks SET lease_until = ?, lease_token = ?", d.Now().UTC().Add(lease), next)
	if err == nil {
		t.LeaseKey = next
	}
	return err
}
func (d *DB) Release(ctx context.Context, t queue.Task) error {
	return d.leasedExec(ctx, t, "UPDATE tasks SET state = 'pending', run_after = ?, lease_worker = NULL, lease_until = NULL, lease_token = NULL", d.Now().UTC())
}

func (d *DB) Stats(ctx context.Context, typ string) (queue.Stats, error) {
	rows, err := d.r(ctx).QueryContext(ctx, "SELECT state, COUNT(*) FROM tasks WHERE type = ? GROUP BY state", typ)
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
	rows, err := d.r(ctx).QueryContext(ctx, "SELECT "+taskCols+" FROM tasks WHERE type = ? AND state = 'dead' ORDER BY created, id", typ)
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
	res, err := d.sql.ExecContext(ctx, "UPDATE tasks SET state = 'pending', attempts = 0, last_error = NULL, run_after = ? WHERE id = ? AND type = ? AND state = 'dead'", d.Now().UTC(), id, typ)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return queue.ErrNotFound
	}
	return nil
}

// ---- scheduler ----

func (d *DB) Lead(ctx context.Context, namespace, worker string, ttl time.Duration) (bool, error) {
	return d.lead(ctx, 1, namespace, worker, ttl)
}

// lead takes or renews lease row kind (1: worker scheduler, 2: backup
// scheduler) of a namespace.
func (d *DB) lead(ctx context.Context, kind int, namespace, worker string, ttl time.Duration) (bool, error) {
	now := d.Now().UTC()
	var lead bool
	err := d.backgroundTx(ctx, func(tx *sql.Tx) error {
		nsID, err := d.namespaceID(ctx, tx, namespace)
		if err != nil {
			return err
		}
		var holder string
		var until time.Time
		err = tx.QueryRowContext(ctx, "SELECT holder, until FROM scheduler_lease WHERE id = ? AND namespace_id = ? FOR UPDATE", kind, nsID).Scan(&holder, &until)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			_, err = tx.ExecContext(ctx, "INSERT INTO scheduler_lease (id, namespace_id, holder, until) VALUES (?, ?, ?, ?)", kind, nsID, worker, now.Add(ttl))
			lead = err == nil
			return err
		case err != nil:
			return err
		case holder == worker || !until.After(now):
			_, err = tx.ExecContext(ctx, "UPDATE scheduler_lease SET holder = ?, until = ? WHERE id = ? AND namespace_id = ?", worker, now.Add(ttl), kind, nsID)
			lead = err == nil
			return err
		}
		lead = false
		return nil
	})
	return lead, err
}

func (d *DB) Due(ctx context.Context, namespace, name string, every time.Duration) (bool, error) {
	now := d.Now().UTC()
	var due bool
	err := d.backgroundTx(ctx, func(tx *sql.Tx) error {
		nsID, err := d.namespaceID(ctx, tx, namespace)
		if err != nil {
			return err
		}
		var last time.Time
		err = tx.QueryRowContext(ctx, "SELECT last_run FROM scheduler WHERE name = ? AND namespace_id = ? FOR UPDATE", name, nsID).Scan(&last)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil && now.Sub(last) < every {
			due = false
			return nil
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO scheduler (name, namespace_id, last_run) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE last_run = VALUES(last_run)", name, nsID, now)
		due = err == nil
		return err
	})
	return due, err
}

var _ queue.Store = (*DB)(nil)

// EnqueuePeriodic coalesces repeat scheduler ticks while the same periodic job
// is pending or leased. It is not suitable for event-triggered work: an event
// arriving during execution needs a successor, whereas another periodic tick
// may be skipped. Dead-letter tasks release their key for the next interval.
func (d *DB) EnqueuePeriodic(ctx context.Context, typ, scope string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	key := sha256.Sum256([]byte(typ + "\x00" + scope))
	now := d.Now().UTC()
	nsID := d.taskNamespaceID(ctx, d.sql, b)
	_, err = d.sql.ExecContext(ctx, "INSERT INTO tasks (id,type,namespace_id,state,payload,run_after,attempts,created,periodic_key) VALUES (?,?,?,'pending',?,?,0,?,?) ON DUPLICATE KEY UPDATE id=id", uuid.NewString(), typ, nsID, string(b), now, now, key[:])
	return err
}
