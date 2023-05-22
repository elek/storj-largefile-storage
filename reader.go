package largefile

import (
	"database/sql"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"storj.io/storj/storagenode/blobstore"
	"storj.io/storj/storagenode/blobstore/filestore"
)

type reader struct {
	offset     int64
	size       int64
	source     *os.File
	virtualPos int64
}

var _ blobstore.BlobReader = &reader{}

func NewReader(db *sql.DB, dir string, ref blobstore.BlobRef) (*reader, error) {
	var file string
	var size int64
	var offset int64

	rows, err := db.Query("select file,slots.size,start from pieces JOIN slots ON pieces.slot_id = slots.id where namespace=$1 AND key=$2 AND NOT trash", ref.Namespace, ref.Key)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, os.ErrNotExist
	}
	err = rows.Scan(&file, &size, &offset)
	if err != nil {
		return nil, err
	}
	source, err := os.Open(filepath.Join(dir, file))
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		source.Seek(offset, 0)
	}
	return &reader{
		source: source,
		size:   size,
		offset: offset,
	}, nil
}

func NewReaderFromEntry(dir string, sourceFile string, size int64, offset int64) (*reader, error) {
	source, err := os.Open(filepath.Join(dir, sourceFile))
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		source.Seek(offset, 0)
	}
	return &reader{
		source: source,
		size:   size,
		offset: offset,
	}, nil
}

func (r *reader) Read(p []byte) (n int, err error) {
	if r.virtualPos > r.size {
		return 0, io.EOF
	}
	read, err := r.source.Read(p)
	if int64(read)+r.virtualPos > r.size {
		read = int(r.size - r.virtualPos)
	}
	r.virtualPos += int64(read)
	return read, err
}

func (r *reader) ReadAt(p []byte, off int64) (n int, err error) {
	at, err := r.source.ReadAt(p, off)
	r.virtualPos = off + int64(at)
	if int64(at)+r.virtualPos > r.size {
		at = int(r.size - r.virtualPos)
	}
	return at, err
}

func (r *reader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		r.virtualPos = r.offset
		return r.Seek(offset+r.offset, whence)

	case 1:
		r.virtualPos += offset
		return r.Seek(offset, whence)
	default:
		return 0, errors.New("Unsupported whence")
	}
}

func (r *reader) Close() error {
	return r.source.Close()
}

func (r *reader) Size() (int64, error) {
	return r.size, nil
}

func (r *reader) StorageFormatVersion() blobstore.FormatVersion {
	return filestore.FormatV1
}
