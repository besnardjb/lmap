# lmap: A Slurm Launcher for Colocated Jobs

lmap is an open-source tool designed to launch colocated jobs (Multi-Program Multi-Data, MPMD) on a Slurm cluster using a scale-invariant syntax. This allows users to allocate resources without heavily relying on specific system resources such as cores from a particular NUMA domain. The primary goal of lmap is to create a portable resource allocation syntax that can be easily mapped onto the target system, enabling efficient performance for modern HPC payloads.

## Usage:
```bash
lmap [OPTIONS] [JOB]
```

### Arguments:
- `[JOB]`: The jobfile specifying the resources to allocate and commands to execute.

### Options:
- `-m, --map`: Output mapping information for the current process.
- `-d, --display`: Display mapping information for the current process.
- `-h, --help`: Print help message.

## Jobfile Syntax
The jobfile is a simple YAML file containing mappings of resources to commands. For example:

```yaml
- map: Enode
  command: ["hostname"]
- map: Aslot
  command: ["echo", "toto"]
```
In this syntax, `map` specifies the resource allocation using a combination of specifiers and levels, while `command` is the command to be executed on those resources.

### Mapping Syntax

A [simulator](https://dynamic-resource.github.io/project/grammar/) is available to experiment with the syntax.

The mapping syntax consists of three components: specifiers (A, E, [0-9]+), levels (Node, Numa, Slot), and separators (comma). Here's how they work together:

| Specifier | Meaning                   | Example        |
|-----------|---------------------------|----------------|
| A         | Equal sharing among jobs  | Anode          |
| E         | One slot from each level | Enode           |
| [0-9]+    | Fixed number of resources | 4slot           |

### Mapping Logic
The mapping algorithm follows these steps:
1. The "each" specifier (E) allocates one slot per dedicated level, starting from the highest level to the lower ones (Node, Numa, Slot).
2. The "fixed" specifier ([0-9]+) allocates a given number of slots iterating at the granularity of the specified resource level.
3. The "all" specifier (A) splits resources between the remaining processes as evenly as possible using a scatter policy based on the target level.