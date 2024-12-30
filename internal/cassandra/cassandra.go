package cassandra

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"pktloader/internal/parquet"

	"github.com/gocql/gocql"
)

type CassandraLoader struct {
	Compress bool
	Debug    bool
	Seeds    string
	KS       string
	Table    string
	Timeout  int
	Retries  int
	Conns    int
	DC       string
	Username string
	Password string
	Errors   atomic.Uint64

	request string
	session *gocql.Session
}

func New() *CassandraLoader {
	return &CassandraLoader{}
}

func (cl *CassandraLoader) Prepare(pkt *parquet.Parquet) error {
	var (
		cname       string
		kind        string
		ctype       string
		columns     string
		columnsFill string
	)

	// cassandra init
	cluster := gocql.NewCluster(cl.Seeds)
	cluster.Keyspace = cl.KS
	cluster.Consistency = gocql.Any // we don't want to wait
	cluster.ProtoVersion = 4        // null handling
	cluster.Timeout = time.Duration(cl.Timeout) * time.Millisecond
	cluster.WriteTimeout = time.Duration(cl.Timeout) * time.Millisecond
	cluster.NumConns = cl.Conns // theoricitally handled by the scylla driver
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: cl.Retries}
	if cl.DC != "" {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(cl.DC))
	} else {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	}
	if cl.Compress {
		cluster.Compressor = &gocql.SnappyCompressor{} // only compressor supported
	}

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: cl.Username,
		Password: cl.Password,
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}

	// session is goroutine safe
	cl.session = session

	// construct insert query

	// get column from cassandra to cross check with parquet schema
	columnType := make(map[string]string)

	req := "SELECT column_name, kind, type FROM system_schema.columns where keyspace_name = '%s' and table_name = '%s'"
	iter := cl.session.Query(fmt.Sprintf(req, cl.KS, cl.Table)).Consistency(gocql.LocalQuorum).Iter()
	for iter.Scan(&cname, &kind, &ctype) {
		columnType[cname] = ctype
	}

	// cross check parquet columns with c* schema
	if len(columnType) != len(pkt.Schema.Fields()) {
		return fmt.Errorf("columns number not matching")
	}

	// get columns from schemas (columns order from parquet file)
	for _, field := range pkt.Schema.Fields() {
		fname := field.Name()
		ftype := field.Type().String()
		ctype := columnType[fname]

		// only 3 types checked
		if ftype == "STRING" && ctype != "text" {
			return fmt.Errorf("column %s invalid type", fname)
		}
		if ftype == "INT32" && ctype != "int" {
			return fmt.Errorf("column %s invalid type", fname)
		}
		if ftype == "DOUBLE" && ctype != "double" {
			return fmt.Errorf("column %s invalid type", fname)
		}

		columns = columns + fname + ","
		columnsFill = columnsFill + "?,"
	}

	columns = strings.Trim(columns, ",")
	columnsFill = strings.Trim(columnsFill, ",")

	// insert reqyest
	cl.request = "INSERT INTO " + cl.KS + "." + cl.Table +
		" (" + columns + ") VALUES (" + columnsFill + ")"
	if cl.Debug {
		fmt.Printf("(debug) query: %s \n", cl.request)
	}

	return nil
}

func (cl *CassandraLoader) Load(v []any) {
	err := cl.session.Query(cl.request).Bind(v...).Exec()
	if err != nil {
		cl.Errors.Add(1)
		if cl.Debug {
			fmt.Printf("(debug) query error: %v\n", err)
		}
	}
}
