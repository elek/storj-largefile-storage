package main

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"os"
)

func init() {
	cmd := cobra.Command{
		Use: "stat",
		RunE: func(cmd *cobra.Command, args []string) error {
			return stat()
		},
	}
	RootCmd.AddCommand(&cmd)

}

func stat() error {
	c := os.Getenv("STORJ_LARGEFILE_CONN")
	conn, err := sql.Open("pgx", c)
	if err != nil {
		return errors.WithStack(err)
	}
	defer conn.Close()

	rows, err := conn.Query("SELECT distinct namespace from pieces")
	if err != nil {
		return errors.WithStack(err)
	}

	var namespace []byte
	for rows.Next() {
		err = rows.Scan(&namespace)
		if err != nil {
			return errors.WithStack(err)
		}
		fmt.Println(hex.EncodeToString(namespace))

	}
	return nil
}
