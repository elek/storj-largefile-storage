package largefile

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"os"
	"path/filepath"
	"storj.io/common/context2"
	"storj.io/common/testcontext"
	"storj.io/private/dbutil/pgutil"
	"storj.io/storj/storagenode/blobstore"
	"storj.io/storj/storagenode/blobstore/filestore"
	"testing"
	"time"
)

func TestStores(t *testing.T, e func(ctx context.Context, t *testing.T, store blobstore.Blobs)) {
	ctx := testcontext.New(t)
	defer ctx.Cleanup()
	t.Run("filestore", func(t *testing.T) {
		store, err := filestore.NewAt(zaptest.NewLogger(t), ctx.Dir("store"), filestore.DefaultConfig)
		require.NoError(t, err)
		e(ctx, t, store)
	})
	t.Run("largestore", func(t *testing.T) {
		TestWithDb(t, ctx, func(ctx context.Context, store *LargeFileStore) {
			e(ctx, t, store)
		})
	})
}

func TestWithDb(t *testing.T, ctx context.Context, test func(ctx context.Context, store *LargeFileStore)) {

	schemaName := "largefile-" + pgutil.CreateRandomTestingSchemaName(8)
	c := os.Getenv("STORE_TEST_CONN")
	if c == "" {
		c = "postgres://postgres@localhost:5432/storage"
	}
	fmt.Println(schemaName)
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
	test(ctx, store)
}
