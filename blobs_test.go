package largefile

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/require"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"storj.io/common/context2"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/private/dbutil/pgutil"
	"storj.io/storj/storage"
	"testing"
	"time"
)

func TestMerge(t *testing.T) {

	ctx := testcontext.New(t)
	defer ctx.Cleanup()

	schemaName := "largefile-" + pgutil.CreateRandomTestingSchemaName(8)
	c := os.Getenv("STORE_TEST_CONN")
	if c == "" {
		c = "postgres://postgres@localhost:5432/storage"
	}
	connStrWithSchema := pgutil.ConnstrWithSchema(c, schemaName)

	storeDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(storeDir, "storage-largefile-verification"), []byte("test"), 0644))
	store, err := NewBlobStore(connStrWithSchema, storeDir)
	require.NoError(t, err)

	require.NoError(t, pgutil.CreateSchema(ctx, store.conn, schemaName))
	defer func(db *sql.DB) error {
		childCtx, cancel := context.WithTimeout(context2.WithoutCancellation(ctx), 15*time.Second)
		defer cancel()
		return pgutil.DropSchema(childCtx, db, schemaName)
	}(store.conn)

	defer store.Close()
	err = InitTable(store.conn)
	require.NoError(t, err)

	f1 := make([]byte, 4000)
	rand.Read(f1)

	f2 := make([]byte, 5000)
	rand.Read(f2)

	ref1 := storage.BlobRef{
		testrand.Bytes(32),
		testrand.Bytes(32),
	}
	ref2 := storage.BlobRef{
		testrand.Bytes(32),
		testrand.Bytes(32),
	}

	save := func(ref storage.BlobRef, content []byte) {
		out, err := store.Create(ctx, ref, int64(len(content)))
		require.NoError(t, err)
		n, err := out.Write(content)
		require.NoError(t, err)
		require.Equal(t, n, len(content))
		require.NoError(t, out.Commit(ctx))
	}
	save(ref1, f1)
	save(ref2, f2)

	err = store.Merge(ctx, ref1, ref2)
	require.NoError(t, err)

	read := func(ref storage.BlobRef) []byte {
		in, err := store.Open(ctx, ref)
		defer in.Close()
		require.NoError(t, err)
		raw, err := io.ReadAll(in)
		require.NoError(t, err)
		return raw
	}

	require.Equal(t, read(ref1), f1)
	require.Equal(t, read(ref2), f2)
}

func TestMoveToTrash(t *testing.T) {
	TestStores(t, func(ctx context.Context, store storage.Blobs) {

		ref1 := storage.BlobRef{
			Namespace: []byte("ns"),
			Key:       []byte("key1"),
		}

		out, err := store.Create(ctx, ref1, 10)
		require.NoError(t, err)
		_, err = out.Write([]byte("1234567890"))
		require.NoError(t, err)
		require.NoError(t, out.Commit(ctx))

		require.NoError(t, store.Trash(ctx, ref1))

		_, err = store.Open(ctx, ref1)
		require.Error(t, err)

		trash, err := store.RestoreTrash(ctx, []byte("ns"))
		require.NoError(t, err)

		require.Equal(t, len(trash), 1)

		a, err := store.Open(ctx, ref1)
		require.NoError(t, err)

		all, err := io.ReadAll(a)
		require.NoError(t, err)

		require.Equal(t, []byte("1234567890"), all)
	})
}

func TestWriteWithSeek(t *testing.T) {
	TestStores(t, func(ctx context.Context, store storage.Blobs) {
		ref1 := storage.BlobRef{
			Namespace: []byte("ns"),
			Key:       []byte("key1"),
		}

		out, err := store.Create(ctx, ref1, 10)
		require.NoError(t, err)

		_, err = out.Seek(10, io.SeekStart)
		require.NoError(t, err)

		_, err = out.Write([]byte("1234567890"))
		require.NoError(t, err)

		_, err = out.Write([]byte("abcdefghijkl"))
		require.NoError(t, err)

		_, err = out.Seek(0, io.SeekStart)
		require.NoError(t, err)

		_, err = out.Write([]byte("ST"))
		require.NoError(t, err)

		_, err = out.Write([]byte("MNB"))
		require.NoError(t, err)

		_, err = out.Seek(31, io.SeekStart)
		require.NoError(t, err)

		require.NoError(t, out.Commit(ctx))

		a, err := store.Open(ctx, ref1)
		require.NoError(t, err)
		defer a.Close()

		all, err := io.ReadAll(a)
		require.NoError(t, err)

		require.Equal(t, []byte("STMNB\x00\x00\x00\x00\x001234567890abcdefghijk"), all)
	})
	//

}

func TestMultiWrite(t *testing.T) {
	TestStores(t, func(ctx context.Context, store storage.Blobs) {
		ref := storage.BlobRef{
			Namespace: []byte("ns"),
			Key:       []byte("key1"),
		}
		create, err := store.Create(ctx, ref, -1)
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			_, err = create.Write([]byte("test"))
			require.NoError(t, err)
		}

		_, err = create.Write([]byte(""))
		require.NoError(t, err)

		err = create.Commit(ctx)
		require.NoError(t, err)

		reader, err := store.Open(ctx, ref)
		require.NoError(t, err)
		defer reader.Close()
		content, err := rall(reader)
		require.NoError(t, err)
		require.Equal(t, 10*4, len(content))
		require.Equal(t, "testtesttesttesttesttesttesttesttesttest", string(content))
	})
}

func rall(r io.Reader) ([]byte, error) {
	b := make([]byte, 0, 1)
	for {
		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, err
		}
	}
}
