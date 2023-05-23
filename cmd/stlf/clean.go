package main

import (
	"database/sql"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
)

func init() {
	cmd := cobra.Command{
		Use: "clean",
		RunE: func(cmd *cobra.Command, args []string) error {
			return clean(args[0])
		},
	}
	RootCmd.AddCommand(&cmd)

}

func clean(dir string) error {
	c := os.Getenv("STORJ_LARGEFILE_CONN")
	conn, err := sql.Open("pgx", c)
	if err != nil {
		return errors.WithStack(err)
	}
	defer conn.Close()

	rows, err := conn.Query("select file from (select slots.file,max(pieces.slot_id) as max from slots LEFT JOIN pieces on pieces.slot_id = slots.id and pieces.trash = false group by slots.file) a where max is null")
	if err != nil {
		return errors.WithStack(err)
	}

	var fileName string
	for rows.Next() {
		err = rows.Scan(&fileName)
		if err != nil {
			return errors.WithStack(err)
		}
		err = os.Remove(filepath.Join(dir, fileName))
		if err != nil {
			return err
		}
		_, err := conn.Exec("DELETE FROM slots where file = $1", fileName)
		if err != nil {
			return errors.WithStack(err)
		}

	}
	return nil
}
