package mariadb

import (
	"context"
	"time"

	"kittiwake/store/identity"
)

const nodeCols = "id, mode, address, host, version, started, last_seen, expires"

func scanNode(row interface{ Scan(...any) error }) (identity.Node, error) {
	var n identity.Node
	if err := row.Scan(&n.ID, &n.Mode, &n.Address, &n.Host, &n.Version, &n.Started, &n.LastSeen, &n.Expires); err != nil {
		return identity.Node{}, mapErr(err)
	}
	return n, nil
}

// RegisterNode upserts the (mode, address) row; started is kept from the
// first registration of this process (callers pass their own start time).
func (d *DB) RegisterNode(ctx context.Context, n identity.Node) (identity.Node, error) {
	if n.Mode == "" || n.Address == "" {
		return identity.Node{}, identity.ErrInvalid
	}
	now := d.Now().UTC()
	n.LastSeen = now
	if n.Started.IsZero() {
		n.Started = now
	}
	if n.Expires.IsZero() {
		n.Expires = now.Add(30 * time.Second)
	}
	if _, err := d.exec(ctx, "INSERT INTO nodes (mode, address, host, version, started, last_seen, expires) VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE host = VALUES(host), version = VALUES(version), started = VALUES(started), last_seen = VALUES(last_seen), expires = VALUES(expires)",
		n.Mode, n.Address, n.Host, n.Version, n.Started.UTC(), n.LastSeen, n.Expires.UTC()); err != nil {
		return identity.Node{}, mapErr(err)
	}
	return scanNode(d.sql.QueryRowContext(ctx, "SELECT "+nodeCols+" FROM nodes WHERE mode = ? AND address = ?", n.Mode, n.Address))
}

func (d *DB) ListNodes(ctx context.Context, mode string) ([]identity.Node, error) {
	q, args := "SELECT "+nodeCols+" FROM nodes ORDER BY mode, address", []any{}
	if mode != "" {
		q, args = "SELECT "+nodeCols+" FROM nodes WHERE mode = ? ORDER BY address", []any{mode}
	}
	rows, err := d.r().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []identity.Node
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func (d *DB) DeleteNode(ctx context.Context, mode, address string) error {
	_, err := d.exec(ctx, "DELETE FROM nodes WHERE mode = ? AND address = ?", mode, address)
	return mapErr(err)
}

func (d *DB) PurgeNodes(ctx context.Context, cutoff time.Time) (int, error) {
	res, err := d.exec(ctx, "DELETE FROM nodes WHERE expires < ?", cutoff.UTC())
	if err != nil {
		return 0, mapErr(err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}
