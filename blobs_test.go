package largefile

import (
	"context"
	"github.com/stretchr/testify/require"
	"io"
	"storj.io/storj/storagenode/blobstore"
	"testing"
)

func TestMoveToTrash(t *testing.T) {
	TestStores(t, func(ctx context.Context, t *testing.T, store blobstore.Blobs) {

		ref1 := blobstore.BlobRef{
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
	TestStores(t, func(ctx context.Context, t *testing.T, store blobstore.Blobs) {
		ref1 := blobstore.BlobRef{
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

}

func TestMultiWrite(t *testing.T) {
	TestStores(t, func(ctx context.Context, t *testing.T, store blobstore.Blobs) {
		ref := blobstore.BlobRef{
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
