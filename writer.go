package largefile

import (
	"context"
	"database/sql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"storj.io/storj/storage"
	"storj.io/storj/storage/filestore"
)

type writer struct {
	ref       storage.BlobRef
	conn      *sql.DB
	output    *os.File
	filePath  string
	committed bool
}

func NewWriter(db *sql.DB, dir string, ref storage.BlobRef) (*writer, error) {
	fileName := filepath.Join(dir, RefToFile(ref))
	_ = os.MkdirAll(filepath.Dir(fileName), 0755)
	output, err := os.Create(fileName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &writer{
		conn:     db,
		ref:      ref,
		output:   output,
		filePath: fileName,
	}, nil
}
func (w *writer) Seek(offset int64, whence int) (int64, error) {
	return w.output.Seek(offset, whence)
}

func (w *writer) Cancel(ctx context.Context) error {
	if w.committed {
		return nil
	}
	_ = w.output.Close()
	return os.Remove(w.filePath)
}

func (w *writer) Commit(ctx context.Context) error {
	if w.committed {
		return errors.New("Too much commit")
	}
	w.committed = true
	stat, err := w.output.Stat()
	if err != nil {
		return err
	}

	err = w.output.Close()
	if err != nil {
		return err
	}

	var id int64
	err = w.conn.QueryRow("INSERT INTO slots (file,size,start) VALUES ($1,$2,0) RETURNING id",
		RefToFile(w.ref),
		stat.Size()).Scan(&id)
	if err != nil {
		return err
	}

	_, err = w.conn.Exec("INSERT INTO pieces (namespace,key,size,slot_id) VALUES ($1,$2,$3,$4)",
		w.ref.Namespace,
		w.ref.Key,
		stat.Size(),
		id)
	return errors.WithStack(err)

}

func (w *writer) Size() (int64, error) {
	stat, err := w.output.Stat()
	return stat.Size(), err
}

func (w *writer) StorageFormatVersion() storage.FormatVersion {
	return filestore.FormatV1
}

func (w *writer) Write(p []byte) (n int, err error) {
	return w.output.Write(p)
}
