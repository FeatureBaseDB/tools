#  dx

`dx` is a load-testing tool used to measure the differences between Pilosa versions. This is typically used to compare a version of Pilosa in development and the last known-good version of Pilosa for any regressions or improvements. Alternately, `dx` can be used to just perform a heavy ingest or query load on a single cluster to see how it performs under load.

## Invocation

```
dx [command] [flags]
```

`dx` can only be used when the two Pilosa clusters are already running. You can then specify the configuration using the following global flags:

```
  -o  --hosts        strings    Comma-separated list of 'host:port' pairs (default localhost:10101)
  -h, --help                    help for dx
  -t, --threadcount  int        Number of concurrent goroutines to allocate (default 1)
  -v, --verbose      bool       Enable verbose logging (default true)
  -d, --datadir      string     Data directory to store resuls (default ~/dx)
```

Use one `--hosts` flag for each cluster. Ex.

```
dx [command] --hosts host1,host2 --hosts host3 --hosts host4,host5,host6
```

is interpreted as cluster0 having hosts host1 and host2, cluster1 having host3, and cluster2 having host4, host5, and host6.

## Commands

Along with the flags, the following commands are used by `dx` to determine what to do:

* `ingest`  --- ingest data from an `imagine` specs file on all clusters
* `query`   --- generate and run queries on all clusters
* `compare` --- compare the results from a `dx ingest` or `dx compare` command

### ingest

Aside from the global flags, the following flags can be used for `dx ingest`:

```
  -h, --help                    help for dx ingest
  -p, --prefix       string     Prefix to use for index (default "dx-")
      --specsfiles   strings    Path to imagine specs file
```

The `ingest` command requires one or more [`imagine` specs file](https://github.com/pilosa/tools/tree/master/imagine) that describes its workload in order to generate data.

Sample ingest:

```
> dx ingest --specsfiles specs.toml --hosts localhost:10101 --hosts localhost:10102
```

will result in the two files (named `0` and `1`) written to a folder in `--datadir`. The folder is named "ingest-{timestamp}" (ex. ingest-2019-07-15T12/59/24-05/00). The files contain a single JSON describing the results of the ingest.

```
{"type":"ingest","time":"635.162153ms","threadcount":1}
```

### query

Aside from the global flags, the following flags can be used for `dx query`:

```
  -q, --queries       int      Number of queries to run (default 100)
  -r, --rows          int      Number of rows to perform intersect query on (default 2)
  -i, --indexes       strings  Indexes to run queries on
  -a, --actualresults bool     Save actual results of queries instead of counts (default false)
      --querytemplate string   Run the queries from a previous result file
```

To compare a current query benchmark to an older one, usae `dx query` with the `--querytemplate` set to the old result so that the queries ran on the newer cluster will be the same. If `--querytemplate` is not set, then `dx` automatically generates `--queries` number of queries using the indexes from `indexes`. If `indexes` is also not specified, then `dx` will default to using all of the indexes present in the first cluster.

Sample query:
```
> dx query --hosts localhost:10101 --hosts localhost:10102 --hosts localhost:8000 --threadcount=4
```

will result in the three files (named `0`, `1`, and `2` in order of the flags) written to the folder "query-{timestamp}" in `--datadir`. The files contain `--queries + 1` number of JSON objects. The objects describe the queries and their results, while the last object describes the total time the whole run took.

```
{"type":"query","time":"532.164µs","threadcount":1,"query":{"id":0,"query":1,"index":"dx-users","field":"numbers","rows":[21,51],"time":"532.164µs","resultcount":82}}
...
{"type":"query","time":"1.275702ms","threadcount":14,"query":{"id":0,"query":0,"index":"imaginary-users","field":"numbers","rows":[1,0],"time":"1.275702ms","resultcount":7}}
{"type":"total","time":"164.410886ms","threadcount":4,"query":{"id":-1,"query":0,"index":"","field":"","rows":null,"time":"164.410886ms"}}
```

### compare

The JSON files output by `dx ingest` and `dx query` are not actually meant to be read by humans. The final step in comparing results between different clusters is `dx compare`.

`dx compare` does not take any flags, but it takes two arguments that specify the paths of the two result files to compare. These two result files must be of the same type, or `dx` will return an error. If the two files are valid, `dx` will automatically determine whether they are of type ingest or query and perform the appropriate comparisons.

### default behavior

If no commands are specified, `dx` checks that the clusters are running and prints out their information.
```
> dx

dx is a tool used to analyze accuracy and performance regression across Pilosa versions.
The following checks whether the clusters specified by the hosts flag are running.

Cluster with hosts localhost:10101
server memory: 16GB [16384MB]
server CPU: Intel(R) Core(TM) i7-6567U CPU @ 3.30GHz
[2 physical cores, 4 logical cores available]
cluster nodes: 1

```
