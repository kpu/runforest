# runforest

A python library that manages and runs experimental pipelines.  Pipeline steps can take a long time to run, so it is important to save output in files.  However, pipelines change constantly and it is hard to keep track of the correct files to use.  Moreover, there can be multiple versions of the same output, corresponding to slighly different pipelines or different configurations.  This library uses hashing: the input files and command lines are hashed together and outputs are stored in a directory named after the hash.  

## API

### Bulidling pipelines

Pipeline steps are shell commands constructed by calling the `stdout` or `step` function with command-line arguments.
```python
stdout("sort", Input("text"))
```
This creates a task with the command line
```bash
sort text
```
and captures its stdout.  The `Input("text")` code annotates `text` as a file, so content matters rather than its name.  

Pipeline steps can use the output from another step:
```python
sorted_text = stdout("sort", Input("text"))
unique_text = stdout("uniq", sorted_text)
```

The more-general `step` function allows multiple outputs, returning them in the same order as they were specified.
```python
(sort, uniq) = step("sort", Input("text"), "|tee", Output("sorted"), "|uniq >", Output("unique"))
```

### Running pipelines
Steps are not run immediately; the `stdout` and `step` functions just build a graph of tasks to run.  To run a pipeline, specify a working directory, a number of parallel tasks, and any number of targets:
```python
run("work", 2, sorted_text, unique_text).wait()
```
`wait()` will return true if all steps succeeded (formally: returned 0 under `set -e -o pipefail` conditions).  One can also add targets to a running pipeline before calling wait.

## Example
See `example/example.py`

## Features
- Unlike domain-specific languages like `make`, one has the full power of Python to write pipelines.
- The command lines themselves are considered part of the version, with the option to ignore parts like sort's memory usage setting.
- Multiple versions are handled efficiently.
- Proper file locking means that separate programs can safely use the same working directory and will not clobber each other.  In fact, they will parallelize where possible.
