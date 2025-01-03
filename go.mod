module pktloader

go 1.23.4

require (
	github.com/gocql/gocql v1.7.0
	github.com/jessevdk/go-flags v1.6.1
	github.com/parquet-go/parquet-go v0.24.0
	go.uber.org/ratelimit v0.3.1
)

require (
	github.com/andybalholm/brotli v1.1.0 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	golang.org/x/sys v0.21.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.14.4
