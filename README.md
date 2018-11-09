Pilosa Tools
===========================================

This repo contains the `pi` tool which can:

- run a variety of predefined benchmarks (that can be configured in various way)
- run combinations of predefined benchmarks
- run benchmarks from multiple "agents" simultaneously
- store the results of complex combinations of benchmarks running on multiple agents locally.

The `pi` tool contains several subcommands which are described in more detail below. To get help for pi, or any subcommand of pi (or any subcommand of any subcommand of pi, etc.), just append `--help` at the command line: e.g. `pi --help` or `pi bench --help` or `pi bench import --help`.


## bench

The bench command has a set of subcommands, one for each available benchmark. All of them take a `--hosts` argument which specifies the Pilosa cluster, and a `--agent-num` argument. The agent num argument is mostly used by `pi spawn` and we discuss it in more detail in that section. 

Example:

```
pi bench import --hosts=one.example.com:10101,two.example.com:10101,three.example.com:10101 --iterations=100000 --max-column-id=10000 --max-row-id=1000
```

The above would import 100,000 random bits into the three node Pilosa cluster specified. All bits would have column ID between 0 and 10,000, and row ID between 0 and 1000.

