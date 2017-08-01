Pilosa Tools
===========================================

This repo contains the `pi` tool which can:

- create Pilosa clusters (locally or on remote machines via ssh)
- run a variety of predefined benchmarks (that can be configured in various way)
- run combinations of predefined benchmarks
- run benchmarks from multiple "agents" simultaneously
- store the results of complex combinations of benchmarks running on multiple agents either locally or on S3.
- fetch benchmarking results from S3 and display a summary

The `pi` tool contains several subcommands which are described in more detail below. To get help for pi, or any subcommand of pi (or any subcommand of any subcommand of pi, etc.), just append `--help` at the command line: e.g. `pi --help` or `pi bench --help` or `pi bench import --help`.

bench
----------
The bench command has a set of subcommands, one for each available benchmark. All of them take a `--hosts` argument which specifies the Pilosa cluster, and a `--agent-num` argument. The agent num argument is mostly used by `pi spawn` and we discuss it in more detail in that section. 

Example:
```
pi bench import --hosts=one.example.com:10101,two.example.com:10101,three.example.com:10101 --iterations=100000 --max-column-id=10000 --max-row-id=1000
```
The above would import 100,000 random bits into the three node Pilosa cluster specified. All bits would have column ID between 0 and 10,000, and row ID between 0 and 1000.

spawn
----------
`pi spawn` is the main workhorse for running complex benchmarks. It takes a config file which specified a series of bench commands to run, along with how many agents to run for each of them, and a name for each one. Giving a good name to each configured benchmark is useful when viewing the output.

```
{
    "benchmarks": [
        {
            "num": 2,
            "name": "import",
            "args": ["import", "--max-row-id", "1000", "--max-column-id", "10000","--seed", "0", "--iterations", "1000000", "--index", "myindex", "--frame", "myframe"]
        },
        {
            "num": 1,
            "name": "qwery",
            "args": ["query", "--index", "myindex", "--query", "'Count(Union(Bitmap(rowID=0, frame=myframe), Bitmap(rowID=1, frame=myframe), Bitmap(rowID=3, frame=myframe)))'"]
        }
    ]
}
```
In order to be more reusable, the config file does NOT specify what Pilosa cluster to run against, or any other details about a particular environment - these will be provided to `pi spawn` at the command line. 

The `num` field of each benchmark in the config file controls how many copies of that particular benchmark to spawn. Each will be spawned with a different `--agent-num` argument. Most benchmarks will automatically modulate their configuration based on the agent number given, so that they do interesting things when run as a group. For example, the diagonal-set-bits benchmark offsets the bits it's setting so that each agent sets a different group of bits along the given diagonal.

`pi spawn` will ssh into each agent host, run the given benchmarks, and aggregate all the output.


Example command which spawns a benchmark against a cluster of ubuntu hosts in AWS with a separate agent:
```
pi spawn --spawn-file=./spawn-configs/import.json --output=s3 --copy-binary --goos=linux --pilosa-hosts="node0.benchmark.sandbox.pilosa.com:10101,node1.benchmark.sandbox.pilosa.com:10101,node2.benchmark.sandbox.pilosa.com:10101" --agent-hosts="agent0.benchmark.sandbox.pilosa.com" --ssh-user=ubuntu
```

create
----------

Experimental tool for easily creating Pilosa clusters both locally (using different ports), or on sets of remote hosts via ssh.


bview
----------

Experimental tool for summarizing `pi spawn` results which have been stored in an S3 bucket.

Example:
```
pi bview --bucket-name=benchmarks-pilosa
```
