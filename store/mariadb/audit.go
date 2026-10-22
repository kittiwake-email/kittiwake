package mariadb

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"kittiwake/store/audit"
)

// WriteAdmin inserts the records in one statement.
func (d *DB) WriteAdmin(ctx context.Context, recs []audit.Record) error {
	if len(recs) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString("INSERT INTO audit_admin (time, account, admin, action, client_ip, record) VALUES ")
	args := make([]any, 0, len(recs)*6)
	for i, r := range recs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(?, ?, ?, ?, ?, ?)")
		b, err := json.Marshal(&r)
		if err != nil {
			return err
		}
		args = append(args, r.Time.UTC(), r.Account, r.Admin, r.Action, r.ClientIP, string(b))
	}
	_, err := d.sql.ExecContext(ctx, sb.String(), args...)
	return err
}

// ReadAdmin returns records in the query's day range, oldest first, with
// the row id as cursor.
func (d *DB) ReadAdmin(ctx context.Context, q audit.Query) ([]audit.Record, string, error) {
	if q.Max <= 0 {
		q.Max = 100
	}
	from, to := q.DayRange(d.Now)
	after := int64(0)
	if q.After != "" {
		after, _ = strconv.ParseInt(q.After, 10, 64)
	}
	var sb strings.Builder
	sb.WriteString("SELECT id, record FROM audit_admin WHERE time >= ? AND time < ? AND id > ?")
	args := []any{from, to.Add(24 * time.Hour), after}
	if q.Account != "" {
		sb.WriteString(" AND (account = ? OR admin = ?)")
		args = append(args, strings.ToLower(q.Account), strings.ToLower(q.Account))
	}
	if q.Action != "" {
		sb.WriteString(" AND action = ?")
		args = append(args, q.Action)
	}
	if q.ClientIP != "" {
		sb.WriteString(" AND client_ip = ?")
		args = append(args, q.ClientIP)
	}
	sb.WriteString(" ORDER BY id LIMIT ?")
	args = append(args, q.Max+1)
	rows, err := d.sql.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []audit.Record
	var lastID int64
	for rows.Next() {
		var id int64
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, "", err
		}
		if len(out) == q.Max {
			return out, strconv.FormatInt(lastID, 10), nil
		}
		var r audit.Record
		if err := json.Unmarshal(raw, &r); err != nil {
			continue
		}
		out = append(out, r)
		lastID = id
	}
	return out, "", rows.Err()
}

// PurgeAdminBefore deletes records older than cutoff; returns rows removed.
func (d *DB) PurgeAdminBefore(ctx context.Context, cutoff time.Time) (int, error) {
	res, err := d.sql.ExecContext(ctx, "DELETE FROM audit_admin WHERE time < ?", cutoff.UTC())
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

var _ audit.AdminLog = (*DB)(nil)
