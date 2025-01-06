package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jessevdk/go-flags"

	"pktloader/internal/cassandra"
	"pktloader/internal/parquet"
)

func main() {
	var opts struct {
		File      string `short:"f" long:"file" description:"parquet file" required:"true"`
		Seeds     string `short:"s" long:"seeds" description:"cassandra seeds" required:"true"`
		KS        string `short:"k" long:"keyspace" description:"cassandra keyspace" required:"true"`
		Table     string `short:"t" long:"table" description:"cassandra table" required:"true"`
		DC        string `short:"r" long:"datacenter" description:"cassandra datacenter" default:""`
		Username  string `short:"u" long:"username" description:"cassandra username" default:"cassandra"`
		Password  string `short:"p" long:"password" description:"cassandra password" default:"cassandra"`
		Workers   int    `short:"w" long:"workers" description:"workers numbers" default:"100"`
		InFlight  int    `short:"i" long:"maxinflight" description:"maximum in flight requests" default:"200"`
		Limit     int    `short:"l" long:"ratelimit" description:"rate limit insert per second" default:"10000"`
		ChunkSize int    `short:"c" long:"chunksize" description:"chunk size for reading parquet rows" default:"100"`
		Conns     int    `long:"connections" description:"number of connections by host" default:"20"`
		Retries   int    `long:"retries" description:"number of retry per query" default:"5"`
		Timeout   int    `long:"timeout" description:"timeout of a query in ms" default:"5000"`
		Sampling  int    `long:"sample" description:"every how many qyeries print message rate" default:"10000"`
		Compress  bool   `long:"compress" description:"compress cql queries"`
		Debug     bool   `long:"debug" description:"print debugging messages"`
	}

	if _, err := flags.Parse(&opts); err != nil {
		switch flagsErr := err.(type) {
		case flags.ErrorType:
			if flagsErr == flags.ErrHelp {
				os.Exit(0)
			}
			os.Exit(1)
		default:
			os.Exit(1)
		}
	}

	// parquet init
	pkt := parquet.New()
	pkt.File = opts.File
	pkt.Limit = opts.Limit
	pkt.ChunkSize = opts.ChunkSize
	pkt.Sampling = opts.Sampling
	if opts.Debug {
		pkt.Debug = true
	}

	// read schema from parquet
	err := pkt.ReadSchema()
	if err != nil {
		fmt.Printf("(error) parquet read-schema: %s\n", err)
		os.Exit(1)
	}

	// cassandra loader init
	cl := cassandra.New()
	cl.Seeds = opts.Seeds
	cl.KS = opts.KS
	cl.Table = opts.Table
	cl.DC = opts.DC
	cl.Username = opts.Username
	cl.Password = opts.Password
	cl.Timeout = opts.Timeout
	cl.Retries = opts.Retries
	cl.Conns = opts.Conns
	cl.Compress = opts.Compress
	if opts.Debug {
		cl.Debug = true
	}

	err := cl.Prepare(pkt)
	if err != nil {
		fmt.Printf("(error) cassandra loader prepare: %v\n", err)
		os.Exit(1)
	}

	// sync primitive
	ch := make(chan []any, opts.InFlight)
	wg := &sync.WaitGroup{}

	// loader workers
	wg.Add(opts.Workers)
	for i := 0; i < opts.Workers; i++ {
		go func() {
			defer wg.Done()
			for v := range ch {
				cl.Load(v)
			}
		}()
	}

	// main reading lopps
	start := time.Now()
	pkt.ReadRows(ch)

	// end
	close(ch)
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("%d rows inserted in %s. (%d rows/s). %d failed\n", pkt.Queries, elapsed, pkt.Queries/int(elapsed.Seconds()), cl.Errors.Load())
}
