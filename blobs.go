package largefile

import (
	"context"
	"database/sql"
	"encoding/base32"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"io"
	"os"
	"path/filepath"
	"storj.io/common/storj"
	"storj.io/storj/storage"
	"storj.io/storj/storage/filestore"
	"time"
)

var verificationFileName = "storage-largefile-verification"

var PathEncoding = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

type LargeFileStore struct {
	conn *sql.DB
	dir  string
}

var _ storage.Blobs = &LargeFileStore{}

func NewBlobStore(connDef string, dir string) (*LargeFileStore, error) {
	conn, err := sql.Open("pgx", connDef)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	_ = InitTable(conn)
	//instance := os.Getenv("STORE_INSTANCE")
	return &LargeFileStore{
		dir:  dir,
		conn: conn,
	}, nil

}
func (b *LargeFileStore) Create(ctx context.Context, ref storage.BlobRef, size int64) (storage.BlobWriter, error) {
	return NewWriter(b.conn, b.dir, ref)
}

func (b *LargeFileStore) Open(ctx context.Context, ref storage.BlobRef) (storage.BlobReader, error) {
	return NewReader(b.conn, b.dir, ref)
}

func (b *LargeFileStore) OpenWithStorageFormat(ctx context.Context, ref storage.BlobRef, formatVer storage.FormatVersion) (storage.BlobReader, error) {
	if formatVer != filestore.FormatV1 {
		return nil, errs.New("Unsupported format")
	}
	return b.Open(ctx, ref)
}

func (b *LargeFileStore) Delete(ctx context.Context, ref storage.BlobRef) error {
	_, err := b.conn.Exec("DELETE FROM pieces WHERE namespace=$1 AND key=$2", ref.Namespace, ref.Key)
	return errors.WithStack(err)
}

func (b *LargeFileStore) DeleteWithStorageFormat(ctx context.Context, ref storage.BlobRef, formatVer storage.FormatVersion) error {
	if formatVer != filestore.FormatV1 {
		return errs.New("Unsupported format")
	}
	return b.Delete(ctx, ref)
}

func (b *LargeFileStore) DeleteNamespace(ctx context.Context, ref []byte) (err error) {
	_, err = b.conn.Exec("UPDATE pieces SET deleted = true WHERE namespace = $1", ref)
	// todo delete files
	return errors.WithStack(err)
}

func (b *LargeFileStore) RenameRef(ctx context.Context, ref1 storage.BlobRef, name string) error {
	var currentFileName string
	err := b.conn.QueryRow("SELECT file FROM pieces WHERE namespace = $1 AND key =$2", ref1.Namespace, ref1.Key).Scan(&currentFileName)
	if err != nil {
		return errors.WithStack(err)
	}
	f1 := filepath.Join(b.dir, currentFileName)
	f2 := filepath.Join(b.dir, name)
	dest, err := os.Create(f2)
	if err != nil {
		return err
	}
	defer dest.Close()
	src, err := os.Open(f1)
	if err != nil {
		return err
	}
	defer src.Close()
	_, err = io.Copy(dest, src)
	if err != nil {
		return err
	}
	_, err = b.conn.Exec("UPDATE pieces SET file=$1 WHERE file = $2", name, currentFileName)
	if err != nil {
		return errors.WithStack(err)
	}
	err = os.Remove(f1)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (b *LargeFileStore) Merge(ctx context.Context, ref1 storage.BlobRef, ref2 storage.BlobRef) error {
	f1 := filepath.Join(b.dir, RefToFile(ref1))
	f2 := filepath.Join(b.dir, RefToFile(ref2))
	stat, err := os.Stat(f1)
	if err != nil {
		return err
	}
	dest, err := os.OpenFile(f1, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer dest.Close()
	src, err := os.Open(f2)
	if err != nil {
		return err
	}
	defer src.Close()
	_, err = io.Copy(dest, src)
	if err != nil {
		return err
	}
	_, err = b.conn.Exec("UPDATE pieces SET file=$1,start=$2 WHERE namespace = $3 and key = $4", RefToFile(ref1), stat.Size(), ref2.Namespace, ref2.Key)
	if err != nil {
		return errors.WithStack(err)
	}
	err = os.Remove(f2)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (b *LargeFileStore) Trash(ctx context.Context, ref storage.BlobRef) error {
	_, err := b.conn.Exec("UPDATE pieces SET trash = true WHERE namespace = $1 and key = $2", ref.Namespace, ref.Key)
	return errors.WithStack(err)
}

func (b *LargeFileStore) RestoreTrash(ctx context.Context, namespace []byte) ([][]byte, error) {
	_, err := b.conn.Exec("UPDATE pieces SET trash = false where namespace = $1 AND deleted = false", namespace)
	return [][]byte{}, errors.WithStack(err)
}

func (b *LargeFileStore) EmptyTrash(ctx context.Context, namespace []byte, trashedBefore time.Time) (int64, [][]byte, error) {
	_, err := b.conn.Exec("DELETE FROM pieces where namespace = $1 AND trash = true", namespace)
	return 0, [][]byte{}, errors.WithStack(err)
}

func (b *LargeFileStore) Stat(ctx context.Context, ref storage.BlobRef) (storage.BlobInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (b *LargeFileStore) StatWithStorageFormat(ctx context.Context, ref storage.BlobRef, formatVer storage.FormatVersion) (storage.BlobInfo, error) {
	if formatVer != filestore.FormatV1 {
		return nil, errs.New("Unsupported format")
	}
	return b.Stat(ctx, ref)
}

func (b *LargeFileStore) FreeSpace(ctx context.Context) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (b *LargeFileStore) CheckWritability(ctx context.Context) error {
	return nil
}

func (b *LargeFileStore) SpaceUsedForTrash(ctx context.Context) (res int64, err error) {
	rows, err := b.conn.Query("SELECT count(size) FROM pieces WHERE trash")
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, errors.New("No such result")
	}
	err = rows.Scan(&res)
	return res, err
}

func (b *LargeFileStore) SpaceUsedForBlobs(ctx context.Context) (res int64, err error) {
	rows, err := b.conn.Query("SELECT count(size) FROM pieces WHERE NOT trash")
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, errors.New("No such result")
	}
	err = rows.Scan(&res)
	return res, err
}

func (b *LargeFileStore) SpaceUsedForBlobsInNamespace(ctx context.Context, namespace []byte) (res int64, err error) {
	rows, err := b.conn.Query("SELECT count(size) FROM pieces WHERE NOT TRASH AND namespace=$1", namespace)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, errors.New("No such result")
	}
	err = rows.Scan(&res)
	return res, err
}

func (b *LargeFileStore) ListNamespaces(ctx context.Context) ([][]byte, error) {
	res := make([][]byte, 0)
	rows, err := b.conn.Query("SELECT DISTINCT namespace FROM pieces")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()
	var ns []byte
	for rows.Next() {
		err := rows.Scan(&ns)
		if err != nil {
			return nil, err
		}
		res = append(res, ns)
	}
	return res, nil
}

func (b *LargeFileStore) WalkNamespace(ctx context.Context, namespace []byte, walkFunc func(storage.BlobInfo) error) error {
	rows, err := b.conn.Query("SELECT namespace,key,size FROM pieces WHERE not trash AND namespace=$1", namespace)
	if err != nil {
		return errors.WithStack(err)
	}
	defer rows.Close()
	for rows.Next() {
		b := BlobInfo{
			ref: storage.BlobRef{},
		}
		err = rows.Scan(&b.ref.Namespace, &b.ref.Key, &b.size)
		if err != nil {
			return errors.WithStack(err)
		}
		err := walkFunc(b)
		if err != nil {
			return errors.WithStack(err)
		}

	}

	return nil
}

func (b *LargeFileStore) CreateVerificationFile(ctx context.Context, id storj.NodeID) error {
	return nil
}

func (b *LargeFileStore) VerifyStorageDir(ctx context.Context, id storj.NodeID) error {
	return nil
}

func (b *LargeFileStore) Close() error {
	return b.conn.Close()
}

func RefToFile(ref storage.BlobRef) string {
	return filepath.Join(PathEncoding.EncodeToString(ref.Namespace), PathEncoding.EncodeToString(ref.Key)+".sj1")
}

func InitTable(conn *sql.DB) error {
	_, err := conn.Exec("create table pieces (namespace BYTEA not null, key BYTEA not null, size int NOT NULL DEFAULT 0,trash bool not null default false,slot_id int not null,PRIMARY KEY(namespace, key))")
	if err != nil {
		return err
	}
	_, err = conn.Exec("create table slots (id serial primary key, file text NOT NULL, start int NOT NULL DEFAULT 0, size int NOT NULL DEFAULT 0)")
	if err != nil {
		return err
	}
	return nil
}
