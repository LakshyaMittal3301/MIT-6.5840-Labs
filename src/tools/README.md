# Raft Debugging Helpers (`dstest` & `dslogs`)

Two small Python utilities live in this folder to make iterating on the Raft labs easier:

- `dstest`: run one or more Go tests repeatedly (optionally in parallel) and keep logs.
- `dslogs`: pretty-print Raft logs with color, topic filtering, and columns.

Both scripts use [`typer`](https://typer.tiangolo.com) and [`rich`](https://rich.readthedocs.io). Install them once with:

```bash
pip install typer rich
```

Copy the executables somewhere on your `PATH` (e.g. `~/bin`) and `chmod +x` if needed.

---

## `dstest` – stress a test suite

Wrapper around `go test` that loops tests, runs workers in parallel, and saves logs for inspection.

### Common usage

```bash
# Run a single test 100 times with 4 workers, save logs in ./logs
dstest -p 4 -n 100 -o logs TestReElection3A

# Run several tests in a round-robin fashion
dstest -p 4 -n 25 TestInitialElection3A TestReElection3A TestManyElections3A
```

### Key flags (from `dstest --help`)

- `-p, --workers`: parallel workers (default: 1).
- `-n, --iter`: iterations per test name (default: 10).
- `-o, --output`: directory for logs; defaults to a timestamped folder if omitted.
- `-s, --sequential`: finish all iterations of the first test, then move to the next (default is interleaved).
- `-a, --archive`: save logs for **all** runs instead of only failures.
- `-r/--no-race`: run `go test` with `-race`.
- `-v, --verbose`: increase verbosity (sets `VERBOSE` env variable for Go tests).
- `-l, --loop`: keep running, increasing `--iter` by `--growth` (default 10) after each round.
- `-t, --timing`: record wall/user/system time (macOS `time` output expected).

### What gets written

- Each run writes stdout/stderr to a temp file. Failing runs (or all runs with `--archive`) are copied into the output directory as `<TestName>_<count>.log`.
- Logs are numbered in the order runs finish, which makes it easy to pipe a failure into `dslogs` for debugging:

```bash
dslogs logs/TestReElection3A_13.log
```

---

## `dslogs` – readable Raft logs

Reads Raft logs from stdin or a file, colors them by topic, and can render multiple peers side-by-side.

### Basic flows

```bash
# Stream from a test run
VERBOSE=1 go test -run TestReElection3A | dslogs

# Inspect a saved log file from dstest
dslogs logs/TestReElection3A_13.log
```

### Filtering and layout

- `-j, --just CMIT,PERS`: show only specific topics.
- `-i, --ignore TIMR,LOG2`: drop noisy topics.
- `-c, --columns 5`: render logs in 5 columns (one per server id in the log line).
- `--no-color`: disable color output (color is on by default).

### Topic palette

`dslogs` recognizes these topics out of the box: `TIMR`, `VOTE`, `LEAD`, `TERM`, `LOG1`, `LOG2`, `CMIT`, `PERS`, `SNAP`, `DROP`, `CLNT`, `TEST`, `INFO`, `WARN`, `ERRO`, `TRCE`.

Lines that do not match the expected `time topic peer message` pattern (e.g. panics or raw test output) are printed as-is so failures remain visible.
