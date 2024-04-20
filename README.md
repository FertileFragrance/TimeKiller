# TimeKiller (Chronos and Aion)
a fast white-box snapshot isolation (SI) checker based on timestamps

a [technical report](./technical-report-for-paper-454.pdf) is available to help you have a more comprehensive understanding

## Requirements

* JDK 11 (or above)
* Maven

If you want to reproduce, you don't need to install it at all. Please check the [reproduce section](#Reproduce) directly. TimeKiller program is provided in every experiment. Don't worry because it is only less than 4MB.

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
* `--port, --timeout_delay, --log_ext_flip, --use_cts_as_rtts, --duration_in_memory, --txn_start_gc, --max_txn_in_mem, --gc_interval` are valid options **only** under `online` mode.
* All other options will be ignored.

### HTTP request format

Aion checks transactions sent through HTTP requests. Each request is sent to `http://127.0.0.1:23333/check` (by default) via POST method carrying json data of transactions. If `--use_cts_as_rtts` is set to `true`, the json data has the same format as the key-value histories accepted by Chronos described above. Otherwise, for each transaction in the json array, it needs an additional field `rtts` meaning real-time timestamp, a millisecond timestamp.

Note that the json data must be a json array, even if the request carries only one transaction. This is designed to encourage the use of batch processing.

### Detailed description of options

If you are confused by the numerous configuration options, don’t worry. After reading this section you will have a comprehensive and detailed understanding of them.

**Meta** option:

* `--mode`: Set it to `fast` (by default) or `gc` to run Chronos (offline checking), or set it to `online` to run Aion (online checking). As the name suggests, Chronos won't perform garbage collection (GC) under `fast` mode to save time. While under `gc` mode, Chronos will perform GC every time it checks a certain number of transactions (configured by `--num_per_gc`). Aion always enables GC to avoid unlimited memory growth (configured by multiple options).

**Generic** options (valid for both Chronos and Aion):

* `--enable_session`: If set to `true` (by default), Chronos or Aion will use the `sts` and `cts` of transactions in the input history to check the SESSION axiom. Informally, SESSION indicates that a later transaction must start after the previous transactions in the same session (having the same `sid`). Otherwise, SESSION will not be checked.
* `--initial_value`: It is the value read from a key before any operation writes on that key. In most cases, it is `null` (by default) or `0`, determined by the running database and the processing logic to get the history.

**Chronos** options:

* `--history_path`: It **must be specified** to let Chronos know which history to check. And the file must have `.json` extension.
* `--data_model`: Chronos supports two types of data models of the operations in a history (of course it is extensible to support more), `kv` (by default) and `list`.
* `--fix`: If added, Chronos will try to fix the history containing violations of SI (and SESSION) by modifying `sts` and `cts` of the transactions related to the violations, and outputs a json file named `FIXED-<input_filename>` in the directory of the input file. If no violations are found, nothing will happen.
* `--num_per_gc`: It is used to set how many transactions are checked to perform a GC, so it is valid **only** under `gc` mode. For example, when it is set to `20000` (by default), Chronos will do a GC after checking the 20000th, 40000th, 60000th, etc. transactions.

**Aion** options:

* `--port`: It is the port the HTTP server to receive requests is running on. The default port is `23333`.

The following 2 options configure re-checking.

* `--timeout_delay`: When receiving a new coming transaction with a smaller commit timestamp, Aion will re-check the previously arrived transactions that have a greater start timestamp. But not all these transactions will be re-checked. This option sets a maximum time window. For example, if set to `5000` (by default), Aion will not re-check the transactions that were first checked 5s ago. If a transaction will not be re-checked any more, we say it **expires**.
* `--log_ext_flip`: With this option, Aion will print a log when a flip-flop happens (i.e. Aion overturned the previous judgment of an EXT violation due to re-checking).

The following 2 options configure when a transaction can be GC.

* `--use_cts_as_rtts`: Aion needs a real-time timestamp of each transaction to decide whether to GC it (introduced in the next option). It is intended to be the physical time when the transaction commits. If the database uses physical millisecond timestamps to implement transactions, this option can be set to `true` (default is `false`), and Aion uses the physical part of `cts` as the `rtts`.
* `--duration_in_memory`: When it is going to GC a transaction, Aion will calculate how long the current timestamp is from `rtts` of the transaction. Only when the time set by this option (default is `10000`) is exceeded and the transaction has expired will Aion GC it.

The following 3 options configure how Aion GC transactions.

* `--txn_start_gc`: Only when the number of in-memory transactions reaches the value of this option (default is `10000`) will Aion starts to do a GC. If the number is smaller, no GC will be performed.
* `--max_txn_in_mem`: If the number of in-memory transactions reaches the value of this option (default is `50000`), Aion will immediately call a GC regardless of the time interval (the next option).
* `--gc_interval`: If the number of in-memory transactions is between the values of the above two options, Aion will call a GC periodically according to this option (default is `10000`).

Note that all time related options are in milliseconds.

Note that the transactions in a history should be in session order. Otherwise there is no point in checking SESSION.

## Reproduce

We use [elle-cli](https://github.com/ligurio/elle-cli) to run [Elle](https://github.com/jepsen-io/elle) on key-value and list histories. You don't need to install it yourself. We provide a packaged program `elle-cli.jar` recording its runtime by adding two timestamps when it starts and ends. You need to manually record this runtime if you install it yourself.

As black-box SI checking tools, [PolySI](https://github.com/hengxin/PolySI-PVLDB2023-Artifacts/tree/main/artifact/PolySI) and [Viper](https://github.com/Khoury-srg/Viper) struggle to have comparable performance to our white-box tools. Installing and running them might be a time consuming work, so we don't provide automated scripts for running them. But we provide the data for all the tools. You can try them on a small history following the instructions on their homepages using our history data if you're interested.

For every experiment we provide `TimeKiller.jar` (and `elle-cli.jar` if required) so you don't need to install it in advance. But you need Java 11 (or above) and Linux environment to run the scripts (some may require Python environment).

Data, programs and scripts can be downloaded from [Google Drive](https://drive.google.com/drive/folders/1jU-TADrRDq-SDxyHFN2Hcyk6RnXG0YEq).

### Fig 3

Download the `fig3.zip`, decompress it, enter `fig3/` directory, and run

```sh
./run-chronos.sh
./run-ellekv.sh
```

Note that it may take some time for `elle-cli.jar` to load and exit, so your somatosensory running time may be longer than the actual running time. This applies to all the experiments that use `elle-cli.jar`.

### Fig 4

Download the `fig4.zip`, decompress it and enter `fig4/` directory.

To reproduce Figure 4a, run

```sh
./run-chronoskv.sh
./run-ellekv.sh
```

To reproduce Figure 4b, run

```sh
./run-chronoslist.sh
./run-ellelist.sh
```

### Fig 5

Download the `fig5.zip`, decompress it and enter `fig5/` directory.

To reproduce subfigure a/b/c/d/e/f, run

```sh
./run-{a|b|c|d|e|f}.sh
```

### Fig 6

Download the `fig6.zip`, decompress it and enter `fig6/` directory.

Running the scripts of this experiment requires `python3` (or you can modify `python3` to `python` in the scripts).

To reproduce subfigure a/b/c/d/e/f, run

```sh
./run-{a|b|c|d|e|f}-chronos.sh
./run-{a|b|c|d|e|f}-ellekv.sh
```

Note that this experiment requires there are no other running `java` processes when a script is running. It also means that the scripts cannot be running at the same time.

### Fig 7

Download the `fig7.zip`, decompress it and enter `fig7/` directory.

To reproduce subfigure a/b/c/d/e/f, run

```sh
./run-{a|b|c|d|e|f}.sh
```

### Fig 8

Download the `fig8.zip`, decompress it, enter `fig8/` directory, and run

```sh
./run.sh
```

### Fig 9

