# TimeKiller (Chronos and Aion)
a fast white-box snapshot isolation (SI) checker based on timestamps

## Requirements

* JDK 8 (or above)
* Maven

## Install

Clone this repository, enter the project root directory, and run

```sh
mvn install
mvn package
```

Then you will get `TimeKiller-jar-with-dependencies.jar` in `target/`.

You can move it to anywhere according to your workspace (e.g. the project root directory).

For the sake of simplicity, I strongly recommend renaming it to `TimeKiller.jar`.

## Quick start

Run TimeKiller by specifying a history to check.

```sh
java -jar TimeKiller.jar --history_path examples/quick-start.json
```

The output is like this:

```text
Checking examples/quick-start.json
==========[ Txn Info Statistics ]==========
|  Number of txns:          100036        |
|  Number of sessions:      50            |
|  Maximum key:             999           |
|  Avg num of ops per txn:  5.000000      |
|  Read op percentage:      0.483538      |
|  Write op percentage:     0.516462      |
===========================================
Satisfy SESSION
Satisfy SI
=========[ Time Usage Statistics ]=========
|  Total time:              0.984000   s  |
|  Loading time:            0.781000   s  |
|  Sorting time:            0.015000   s  |
|  Checking time:           0.171000   s  |
===========================================
```

It's pretty _quick_, isn't it? You have successfully checked 100k transactions in only 1 second!

## Customize running

Use the following command to see usage help.

```sh
java -jar TimeKiller.jar --help
```

The whole instructions for use are as follows.

```text
usage: TimeKiller [--data_model <arg>] [--duration_in_memory <arg>] [--enable_session <arg>] [--fix] [--gc_interval <arg>] [-h] [--history_path <arg>]
       [--initial_value <arg>] [--log_ext_flip] [--max_txn_in_mem <arg>] [--mode <arg>] [--num_per_gc <arg>] [--port <arg>] [--timeout_delay <arg>]
       [--txn_start_gc <arg>] [--use_cts_as_rtts <arg>]
    --data_model <arg>           the data model of transaction operations [default: kv] [possible values: kv, list]
    --duration_in_memory <arg>   the duration transaction kept in memory in realtime in millisecond under online mode [default: 10000]
    --enable_session <arg>       whether to check the SESSION axiom using timestamps [default: true]
    --fix                        fix violations if found
    --gc_interval <arg>          the time interval between online gc in millisecond [default: 10000]
 -h,--help                       print usage help and exit
    --history_path <arg>         the filepath of history in json format
    --initial_value <arg>        the initial value of keys before all writes [default: null]
    --log_ext_flip               print EXT flip-flops under online mode
    --max_txn_in_mem <arg>       the max number of in-memory transactions such that online gc is called immediately regardless of the interval [default: 50000]
    --mode <arg>                 choose a mode to run TimeKiller [default: fast] [possible values: fast, gc, online]
    --num_per_gc <arg>           the number of checked transactions for each gc [default: 20000]
    --port <arg>                 HTTP request port for online checking [default: 23333]
    --timeout_delay <arg>        transaction timeout delay of online checking in millisecond [default: 5000]
    --txn_start_gc <arg>         the number of in-memory transactions such that online gc can be called [default: 10000]
    --use_cts_as_rtts <arg>      use the physical part of commit timestamp as realtime timestamp under online mode [default: false]
```

Note that these options cannot be arbitrarily combined as shown in the descriptions. For detail, please see below.

### Run Chronos

Set `--mode` to `fast` or `gc` to run Chronos, performing a one-shot offline checking of an existing history.

Under `fast` or `gc` mode:

* `--history_path` is a **required** option to set the path of json-format history. The file must have `.json` extension.
* `--enable_session, --initial_value, --data_model, --fix` are valid options.

Under `fast` mode:

* All other options will be ignored.

Under `gc` mode:

* `--num_per_gc` is a valid option.
* All other options will be ignored.

### Input file format

For key-value histories, the input json file must have the following format.

```json
[
    {
        "tid": 67,
        "sid": 19,
        "sts": {"p": 1691420095436341, "l": 61},
        "cts": {"p": 1691420095659914, "l": 18},
        "ops": [
            {"t": "w", "k": 7, "v": 2},
            {"t": "w", "k": 4, "v": 5},
            {"t": "r", "k": 8, "v": 3}
        ]
    }
]
```

The whole file is a json array consisting of transaction objects:

* `tid` means transaction id, a unique identifier for each transaction. It can be a string.
* `sid` means session id, a unique identifier for each session. The transaction is executed on this session. It can be a string.
* `sts` means start timestamp, which is a hybrid logical clock (HLC). `p` and `l` respectively stand for the physical and logical part of the timestamp. If the transaction does not have a logical timestamp, it's ok to set `l` to `0`. And `p` is not required to actually represent a physical clock. It can be an incrementing logical tick (usually in which case `l` is `0`). `p` and `l` must be (long) integers.
* `cts` means commit timestamp, which is also a HLC. It must be greater than or equal to the `sts` in a transaction.
* `ops` means operations, an array of operations:
  * `t` means type of the operation. Possible values are `w, r, write, read` (not case sensitive).
  * `k` means key of the operation. It must be a (long) integer.
  * `v` means value of the operation. It must be a (long) integer or `null`. If the latter it can be omitted.

See `examples/format-kv.json` for a complete key-value history sample.

For list histories, the only difference is the format of each operation:

```json
[
    {"t": "a", "k": 3, "v": 1},
    {"t": "r", "k": 6, "v": [4, 5, 9]}
]
```

* `t` can be one of `a, r, append, read` (not case sensitive).
* `k` is the same as above.
* `v` has different formats according to `t`. If the operation is an append then `v` is a (long) integer. Otherwise (the operation is a read), `v` is a list consisting of (long) integers. If `v` is an empty list then it can be set to `null` or be omitted.

See `examples/format-list.json` for a complete list history sample.

### Run Aion

Set `--mode` to `online` to run Aion, performing a continuous online checking by receiving HTTP requests of a history.

Under `online` mode:

* `--enable_session, --initial_value` are valid options just like Chronos.
* `--port, --timeout_delay, --use_cts_as_rtts, --duration_in_memory, --log_ext_flip, --txn_start_gc, --max_txn_in_mem, --gc_interval` are valid options **only** under `online` mode.
* All other options will be ignored.

### HTTP request format

Aion checks transactions sent through HTTP requests. Each request is sent to `http://127.0.0.1:23333/check` (by default) via POST method carrying json data of transactions. If `--use_cts_as_rtts` is set to `true`, the json data has the same format as the key-value histories accepted by Chronos described above. Otherwise, for each transaction in the json array, it needs an additional field `rtts`, a millisecond timestamp.

Note that the json data must be a json array, even if the request carries only one transaction. This is designed to encourage the use of batch processing.

### Detailed description of options

If you are confused by the numerous configuration options, don’t worry. After reading this section you will have a comprehensive and detailed understanding of them.

