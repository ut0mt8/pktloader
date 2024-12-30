package parquet

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/gocql/gocql"
	"github.com/parquet-go/parquet-go"
	"go.uber.org/ratelimit"
)

type Parquet struct {
	File        string
	ParquetFile *parquet.File
	Schema      *parquet.Schema
	Debug       bool
	ChunkSize   int
	Sampling    int
	Limit       int
	Queries     int
}

func New() *Parquet {
	return &Parquet{}
}

func (pkt *Parquet) ReadSchema() error {
	file, err := os.Open(pkt.File)
	if err != nil {
		return fmt.Errorf("open parquet-file: %w", err)
	}

	fstat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat parquet-file: %w", err)
	}

	pkt.ParquetFile, err = parquet.OpenFile(file, fstat.Size())
	if err != nil {
		return fmt.Errorf("open-file parquet-file: %w", err)
	}

	pkt.Schema = pkt.ParquetFile.Schema()

	if pkt.Debug {
		for _, field := range pkt.Schema.Fields() {
			fmt.Printf("(debug) parquet-file schema: %s, %s\n", field.Name(), field.Type().String())
		}
	}

	return nil
}

func (pkt *Parquet) ReadRows(ch chan<- []any) {
	rl := ratelimit.New(pkt.Limit)
	wg := &sync.WaitGroup{}

	// loop over row groups
	for _, rgrp := range pkt.ParquetFile.RowGroups() {
		wg.Add(1)

		go func(rgrp parquet.RowGroup, ch chan<- []any) {
			defer wg.Done()

			reader := parquet.NewRowGroupReader(rgrp)
			defer reader.Close()

			for {
				// read rows by chunkSize
				rows := make([]parquet.Row, pkt.ChunkSize)

				nrows, err := reader.ReadRows(rows)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
				}

				for r := 0; r < nrows; r++ {
					var values []any

					for _, val := range rows[r] {
						// type conversion
						if val.IsNull() {
							values = append(values, &gocql.UnsetValue)
						} else if val.Kind() == parquet.ByteArray {
							values = append(values, val.String())
						} else if val.Kind() == parquet.Int32 {
							values = append(values, val.Int32())
						} else if val.Kind() == parquet.Double {
							values = append(values, val.Double())
						}
					}

					// send to cql workers
					rl.Take()
					ch <- values
					pkt.Queries++

					if pkt.Debug && pkt.Queries%pkt.Sampling == 0 {
						fmt.Printf("(debug) inserted %d (%d)\n", pkt.Queries, len(ch))
					}
				}
			}
		}(rgrp, ch)

		wg.Wait()
	}
}
