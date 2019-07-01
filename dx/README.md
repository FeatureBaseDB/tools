#  dx

`dx` is a load-testing tool used to measure the differences between two Pilosa instances. This is typically used to compare a version of Pilosa in development
(called the **candidate**) and the last known-good version of Pilosa (called the **primary**) for any regressions or improvements.

## Invocation

```
dx [command] [flags]
```

`dx` can only be used when the two Pilosa clusters are already running. You can then specify the configuration using the following global flags:

```
      --chosts       strings    Hosts of candidate instance (default [localhost])
      --cport        int        Port of candidate instance (default 10101)
  -h, --help                    help for dx
      --phosts       strings    Hosts of primary instance (default [localhost])
      --pport        int        Port of primary instance (default 10101)
  -p, --prefix       string     Prefix to use for index (default "dx-")
      --specsfile    string     Path to specs file (default "specs.toml")
  -t, --threadcount  int        Number of concurrent goroutines to allocate (default 1)
  -v, --verbose                 Enable verbose logging
```

## Commands

Along with the flags, the following commands are used by `dx` to determine what to do:

* `ingest`  --- ingest randomly generated data in both Pilosa clusters
* `query`   --- generate and run random queries in both Pilosa clusters

### ingest

The `ingest` command require a specs file (in TOML format) that describes its workload in order to generate data. Otherwise,
it defaults to the provided `specs.toml` file. Currently, `dx` only supports using a single index called `index`. 

To make changes to the specs, it is recommended that changes only be made to the `field`, `min`, `max`, and `column` 
values to scale the workload up or down.

Sample ingest:
```
> dx ingest --cport=10102 --pport=10103 --threadcount=3

     ingest                       primary           candidate     delta
       1000               3m53.057057148s     3m48.850570433s     -1.8%
```

### query

The `query` command should only be ran after an `ingest`. It also uses a specs file to determine which field and rows to make
queries on.

Aside from the global flags, the following flags can be used for `dx query`:

```
  -q, --queries   ints     Number of queries to run (default [100000,1000000,10000000,100000000])
  -r, --rows      int      Number of rows to perform intersect query on (default 2)
```

Sample query:
```
> dx query --cport=10102 --pport=10103 --queries=100,1000,10000 --threadcount=1

     queries     accuracy         primary       candidate     delta
         100       100.0%     0.651 ms/op     0.649 ms/op     -0.3%
        1000       100.0%     0.591 ms/op     0.597 ms/op      1.1%
       10000       100.0%     0.501 ms/op     0.498 ms/op     -0.6%
```

If no commands are specified, `dx` checks that the two servers are running and prints out their information.
```
> dx --cport=10102 --pport=10103

dx is a tool used to measure the differences between two Pilosa instances. The 
following checks whether the two instances specified by the flags are running.

Candidate Pilosa
running on hosts [localhost] and port 10102
server memory: 16GB [16384MB]
server CPU: Intel(R) Core(TM) i7-6567U CPU @ 3.30GHz
[2 physical cores, 4 logical cores available]
cluster nodes: 1

Primary Pilosa
running on hosts [localhost] and port 10103
server memory: 16GB [16384MB]
server CPU: Intel(R) Core(TM) i7-6567U CPU @ 3.30GHz
[2 physical cores, 4 logical cores available]
cluster nodes: 1
```

## solo

Sometimes, it may not be possible to run two clusters simultaneously for testing. In that case, `dx solo` commands can be used to run the desired command on a single cluster and temporarily store the results in the data directory specified by the `datadir` flag until a second command with the same specs file is ran.

When a `solo` command is ran, `dx` will check the data directory for a previously stored result file matching the specs file provided. If the result file exists, `dx` will run the command and compare the result with the result stored in the file. Otherwise, if the file does not exist, `dx` will store the result in a file uniquely associated with the contents of the specs file.

Sample workflow (sequentially ran):

```
> dx solo ingest --specsfile=specs.toml --cport=10101
>>> ingest result is stored in ".dx/.solo/<sha256 hash of specs content>ingest"

> dx solo query --specsfile=specs.toml --cport=10101 --queries=100,100,100,10000,1000,10000,100 --threadcount=4
>>> query result is stored in ".dx/.solo/<sha256 hash of specs content>query<threadcount>"

> dx solo ingest --specsfile=specs.toml --pport=10101
>>> ingest result is compared to previously recorded result and printed out

> dx solo query --specsfile=specs.toml --pport=10101
>>> query result is compared to previously recorded result and printed out
```
