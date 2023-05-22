package main

import (
	"database/sql"
	"fmt"
	largefile "github.com/elek/storj-largefile-storage"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"strings"
)

func init() {
	cmd := cobra.Command{
		Use: "index",
		RunE: func(cmd *cobra.Command, args []string) error {
			return index(args[0])
		},
	}
	RootCmd.AddCommand(&cmd)

}

func index(s string) error {
	c := os.Getenv("STORE_CONN")
	conn, err := sql.Open("pgx", c)
	if err != nil {
		return errors.WithStack(err)
	}
	defer conn.Close()

	_ = largefile.InitTable(conn)
	slotStmt, err := conn.Prepare("INSERT INTO slots (file,size,start) VALUES ($1,$2,0) RETURNING id")
	if err != nil {
		return errors.WithStack(err)
	}

	pieceStmt, err := conn.Prepare("INSERT INTO pieces (namespace,key,size,slot_id) VALUES ($1,$2,$3,$4)")
	if err != nil {
		return errors.WithStack(err)
	}
	var id int64
	nss, err := os.ReadDir(s)
	if err != nil {
		return err
	}
	for _, ns := range nss {
		if !ns.IsDir() {
			continue
		}
		prefixes, err := os.ReadDir(filepath.Join(s, ns.Name()))
		if err != nil {
			return err
		}
		for _, prefix := range prefixes {
			keys, err := os.ReadDir(filepath.Join(ns.Name(), prefix.Name()))
			if err != nil {
				return err
			}
			for _, key := range keys {
				if key.IsDir() {
					continue
				}
				stat, err := os.Stat(filepath.Join(s, ns.Name(), prefix.Name(), key.Name()))
				fmt.Println(ns, key)

				nsBytes, err := largefile.PathEncoding.DecodeString(ns.Name())
				if err != nil {
					return errors.WithStack(err)
				}

				keyBytes, err := largefile.PathEncoding.DecodeString(prefix.Name() + strings.TrimSuffix(key.Name(), ".sj1"))
				if err != nil {
					return errors.WithStack(err)
				}

				err = slotStmt.QueryRow(filepath.Join(ns.Name(), prefix.Name(), key.Name()), stat.Size()).Scan(&id)
				if err != nil {
					return errors.WithStack(err)
				}

				_, err = pieceStmt.Exec(nsBytes, keyBytes, stat.Size(), id)
				if err != nil {
					return errors.WithStack(err)
				}
			}
		}
	}
	return nil
}
