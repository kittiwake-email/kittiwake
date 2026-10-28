package mariadb

import (
	"context"
	"github.com/google/uuid"
)

func (d *DB) EnqueueAttachmentGC(ctx context.Context, storageID int64, id string) error {
	token := uuid.New()
	_, err := d.sql.ExecContext(ctx, "INSERT INTO attachment_gc (storage_id,object_id,token) VALUES (?,?,?) ON DUPLICATE KEY UPDATE token=VALUES(token)", storageID, id, token[:])
	return err
}

// Close each result before callbacks so a one-connection pool can make progress.
// The finite high watermark bounds a pass even as new removals arrive.
func (d *DB) AttachmentGCCandidates(ctx context.Context, storageID int64, fn func(string) (bool, error)) error {
	var ceiling []byte
	if err := d.sql.QueryRowContext(ctx, "SELECT MAX(object_id) FROM attachment_gc WHERE storage_id=?", storageID).Scan(&ceiling); err != nil {
		return err
	}
	if len(ceiling) == 0 {
		return nil
	}
	after := ""
	for {
		rows, err := d.sql.QueryContext(ctx, "SELECT object_id,token FROM attachment_gc WHERE storage_id=? AND object_id>? AND object_id<=? ORDER BY object_id LIMIT 512", storageID, after, ceiling)
		if err != nil {
			return err
		}
		type candidate struct {
			id    string
			token []byte
		}
		var batch []candidate
		for rows.Next() {
			var c candidate
			if err := rows.Scan(&c.id, &c.token); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, c)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, c := range batch {
			done, err := fn(c.id)
			if err != nil {
				return err
			}
			if done {
				if _, err := d.sql.ExecContext(ctx, "DELETE FROM attachment_gc WHERE storage_id=? AND object_id=? AND token=?", storageID, c.id, c.token); err != nil {
					return err
				}
			}
			after = c.id
		}
		if len(batch) < 512 {
			return nil
		}
	}
}
