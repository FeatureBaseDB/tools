Pilosa Tools for Operations and Development
===========================================

cfn-production
--------------

Production Cloudformation template generator. Mostly DNS management.

cfn-sandbox
-----------

Sandbox Cloudformation template generator

cfn-pilosa
----------

Cloudformation tools for creating Pilosa clusters


creator
----------

Go library with helpers for creating pilosa clusters.


ssh
----------

Go library with helpers for accessing other machines via ssh.


build
----------

Go library with helpers for building the pilosa binary.


bench
----------

Go library containing pilosa benchmarks and some helper code.


cmd/pitool
----------

Go program which contains various subcommands for using pilosa, including creating clusters, and running benchmarks.

Run `pitool` for an overview of commands, and `pitool <command> -h` for specific information on that command.

### Create

`pitool create` is used to create pilosa clusters. It has a number of options for controlling how the cluster is configured, what hosts it is on, and even the ability to build the pilosa binary locally and copy it to each cluster node automatically. To start pilosa on remote hosts, you only need `ssh` access to those hosts. See `pitool create -h` for a full list of options.

Examples:

Create a 5 node cluster locally (using 5 different ports), with a replication factor of 2.
```
pitool create \
  -serverN 5 \
  -replicaN 2
```

Create a cluster on 3 remote hosts - all logs will come to local stderr, pilosa binary must be available on remote hosts. The ssh user on the remote hosts needs to be the same as your local user. Otherwise use the `ssh-user` option.
```
pitool create \
  -hosts="node1.example.com:10101,node2.example.com:10101,node3.example.com:10101"
```

Create a cluster on 3 remote hosts running OSX, but build the binary locally and copy it up. Stream the stderr of each node to a separate local log file.
```
pitool create \
  -hosts="mac1.example.com:10101,mac2.example.com:10101,mac3.example.com:10101" \
  -copy-binary \
  -goos=darwin \
  -goarch=amd64 \
  -log-file-prefix=clusterlogs
```

### Bagent

`pitool bagent` is what you want if you just want to run a simple benchmark against an existing cluster. Running it with no arguments will print some help, including the set of subcommands that it may be passed. Calling a subcommand with `-h'` will print the options for that subcommand. The `agent-num` flag can be passed an integer which can change the behavior the benchmarks that are run. This is useful when multiple invocations of the same benchmark are made by the `bspawn` command - they can each (for example) set different bits even though they all have the same arguments.

E.G.
```
pitool bagent \
  -hosts="localhost:10101,localhost:10102" \
  import -h
```

Multiple subcommands and their arguments may be concatenated at the command line and they will be run serially. This is useful (i.e.) for importing a bunch of data, and then executing queries against it.

This will generate and import a bunch of data, and then execute random queries against it.

```
pitool bagent \
  -hosts="localhost:10101,localhost:10102" \
  import -max-bits-per-map=10000 \
  random-query -iterations 100
```

### Bspawn
`pitool bspawn` allows you to automate the creation of clusters and the running of complex benchmarks which span multiple benchmark agents against them. It has a number of options which are described by `pitool bspawn` with no arguments, and also takes a config file which describes the Benchmark itself - this file is described below.

#### Configuration Format

The configuration file is a json object with the top level key `benchmarks`. This contains a list of objects each of which represents a `bagent` command (the `args` key) that will be run some number of times concurrently (the `num` key), and a `name` which should describe the overall effect that command. An example is below.
```json
{
    "benchmarks": [
        {
            "num": 3,
            "name": "set-diags",
            "args": ["diagonal-set-bits", "-iterations", "30000", "-client-type", "round_robin"]
        },
        {
            "num": 2,
            "name": "rand-plus-zipf",
            "args": ["random-set-bits", "-iterations", "20000", "zipf", "-iterations", "100"]
        }
    ]
}
```

All of the benchmarks, and agents are run concurrently. Each agent will be passed an `agent-num` which can modify the behavior in a way that is benchmark specific. See the documentation for each benchmark to see how `agent-num` changes its behavior.
