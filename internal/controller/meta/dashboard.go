package meta

import (
	"context"
	"encoding/json"
)

// DashboardObject is a complete controller-side object snapshot, including
// objects that are still UPLOADING or being deleted. It is intentionally
// read-only and is used by the operator dashboard.
type DashboardObject struct {
	Object Object
	Chunks []ChunkWithReplicas
}

// DashboardObjects returns the metadata mapping needed by the operator view.
// The dashboard is an observability surface, so it includes every object
// state rather than only the client-visible READY listing.
func (s *Store) DashboardObjects(ctx context.Context) ([]DashboardObject, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT object_id, name, size, content_type, checksum_sha256, num_chunks, status,
		       created_at, modified_at, last_activity, metadata_version, custom_metadata
		FROM objects ORDER BY created_at DESC, object_id DESC`)
	if err != nil {
		return nil, err
	}
	var objects []Object
	for rows.Next() {
		var o Object
		var created, modified, activity int64
		var raw string
		if err := rows.Scan(&o.ID, &o.Name, &o.Size, &o.ContentType, &o.ChecksumSHA256,
			&o.NumChunks, &o.Status, &created, &modified, &activity, &o.MetadataVersion, &raw); err != nil {
			rows.Close()
			return nil, err
		}
		o.CreatedAt = fromNanos(created)
		o.ModifiedAt = fromNanos(modified)
		o.LastActivity = fromNanos(activity)
		if err := json.Unmarshal([]byte(raw), &o.CustomMetadata); err != nil {
			rows.Close()
			return nil, err
		}
		objects = append(objects, o)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]DashboardObject, 0, len(objects))
	for _, o := range objects {
		chunks, err := s.ChunkMap(ctx, o.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, DashboardObject{Object: o, Chunks: chunks})
	}
	return out, nil
}
