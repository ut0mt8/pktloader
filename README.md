Go parquet to Cassandra/Scylla fast loader

````
Usage:
  pktloader [OPTIONS]

Application Options:
  -f, --file=        parquet file
  -s, --seeds=       cassandra seeds
  -k, --keyspace=    cassandra keyspace
  -t, --table=       cassandra table
  -r, --datacenter=  cassandra datacenter
  -u, --username=    cassandra username (default: cassandra)
  -p, --password=    cassandra password (default: cassandra)
  -w, --workers=     workers numbers (default: 100)
  -i, --maxinflight= maximum in flight requests (default: 200)
  -l, --ratelimit=   rate limit insert per second (default: 10000)
  -c, --chunksize=   chunk size for reading parquet rows (default: 100)
      --connections= number of connections by host (default: 20)
      --dryrun       only decode parquet
      --retries=     number of retry per query (default: 5)
      --timeout=     timeout of a query in ms (default: 5000)
      --sample=      every how many qyeries print message rate (default: 10000)
      --compress     compress cql queries
      --debug        print debugging messages

Help Options:
  -h, --help         Show this help message
````
