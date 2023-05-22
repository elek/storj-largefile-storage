package main

import (
	"database/sql"
	largefile "github.com/elek/storj-largefile-storage"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs"
	"io"
	"os"
	"storj.io/storj/storage"
)

func init() {
	cmd := cobra.Command{
		Use: "compact",
		RunE: func(cmd *cobra.Command, args []string) error {
			return compact(args[0], args[1])
		},
	}
	RootCmd.AddCommand(&cmd)

}

func compact(dir string, newName string) error {
	c := os.Getenv("STORE_CONN")
	conn, err := sql.Open("pgx", c)
	if err != nil {
		return errors.WithStack(err)
	}
	defer conn.Close()

	store, err := largefile.NewBlobStore(c, dir)
	if err != nil {
		return err
	}
	defer store.Close()

	if _, err = os.Stat(newName); err != nil {
		return errs.New("File already exists.")
	}
	dest, err := os.Create(newName)
	if err != nil {
		return errors.WithStack(err)
	}
	defer dest.Close()

	rows, err := conn.Query("SELECT namespace,key,file,slots.size,start FROM pieces JOIN slots on slots.id = pieces.slot_id WHERE not trash")
	if err != nil {
		return errors.WithStack(err)
	}

	var ref storage.BlobRef
	var size, offset int64
	var sourceFile string

	pos := int64(0)
	for rows.Next() {
		err = rows.Scan(&ref.Namespace, &ref.Key, &sourceFile, &size, &offset)
		if err != nil {
			return errors.WithStack(err)
		}
		reader, err := largefile.NewReaderFromEntry(dir, sourceFile, size, offset)
		if err != nil {
			return errors.WithStack(err)
		}
		_, err = io.Copy(dest, reader)
		reader.Close()
		if err != nil {
			return err
		}

		var id int64
		err = conn.QueryRow("INSERT INTO slots (file,size,start) VALUES ($1,$2,$3) RETURNING id",
			newName,
			size, pos).Scan(&id)
		if err != nil {
			return errors.WithStack(err)
		}

		_, err = conn.Exec("UPDATE pieces SET slot_id = $1 WHERE namespace = $2 AND key = $3",
			id,
			ref.Namespace,
			ref.Key)
		if err != nil {
			return errors.WithStack(err)
		}
		pos += size

	}
	return nil
}
