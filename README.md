# TimeKiller (Chronos and Aion)
a fast white-box snapshot isolation (SI) checker based on timestamps

a technical report is available [here](./1420-icde2025-technical-report.pdf)

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
usage: TimeKiller [--consistency_model <arg>] [--data_model <arg>] [--duration_in_memory <arg>] [--enable_session <arg>] [--fix] [--gc_interval <arg>] [-h]
       [--history_path <arg>] [--initial_value <arg>] [--log_ext_flip] [--max_txn_in_mem <arg>] [--mode <arg>] [--num_per_gc <arg>] [--port <arg>]
       [--timeout_delay <arg>] [--txn_start_gc <arg>] [--use_cts_as_rtts <arg>]
    --consistency_model <arg>    consistency model to check [default: SI] [possible values: SI, SER]
    --data_model <arg>           the data model of transaction operations [default: kv] [possible values: kv, list]
    --duration_in_memory <arg>   the duration transaction kept in memory in realtime in millisecond under online mode [default: 10000]
    --enable_session <arg>       whether to check the SESSION axiom using timestamps [default: true]
    --fix                        fix violations if found
    --gc_interval <arg>          the time interval between online gc in millisecond [default: 10000]
 -h,--help                       print usage help and exit
    --history_path <arg>         the filepath of history in json format
    --initial_txn_path <arg>     the filepath of customized initial transaction
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
* `--consistency_model, --enable_session, --initial_txn_path, --initial_value, --data_model, --fix` are valid options.

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

* `--consistency_model, --enable_session, --initial_txn_path, --initial_value` are valid options just like Chronos.
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

* `--consistency_model`: It is the consistency model (isolation level) to check, which is SI by default. SER is also supported.
* `--enable_session`: If set to `true` (by default), Chronos or Aion will use the `sts` and `cts` of transactions in the input history to check the SESSION axiom. Informally, SESSION indicates that a later transaction must start after the previous transactions in the same session (having the same `sid`). Otherwise, SESSION will not be checked.
* `--initial_txn_path`: It is the path of a json file containing the initial transaction. It is used to set the initial value of keys before any operation writes on them. It is often used when different keys have different initial values.
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

Note that the transactions in a history should be in session order. Otherwise, there is no point in checking SESSION.

## Run end-to-end

### Step 1: Deploy a database

Taking dgraph as an example, [download](https://github.com/hypermodeinc/dgraph/releases) it and execute the following commands to run it on local machine.

```sh
dgraph zero --my=localhost:5080
dgraph alpha --my=localhost:7080 --zero=localhost:5080
```

### Step 2: Run a workload

Enter the `./workloads/` directory and follow the instructions to run a workload (it may be cumbersome to set up the environment for running our default workload using dbcdc-runner).

Once the workload finishes, you will get a json file containing the history of transactions (with another json file recording the initial transaction if running TPCC, RUBiS or Twitter).

### Step 3: Run Chronos or Aion

Run Chronos or Aion with the history file you get in the previous step. You can explore more options to customize the running.

## Reproduce

We use [elle-cli](https://github.com/ligurio/elle-cli) to run [Elle](https://github.com/jepsen-io/elle) on key-value and list histories. You don't need to install it yourself. We provide a packaged program `elle-cli.jar` recording its runtime by adding two timestamps when it starts and ends. You need to manually record this runtime if you install it yourself.

As black-box SI checking tools, [PolySI](https://github.com/hengxin/PolySI-PVLDB2023-Artifacts/tree/main/artifact/PolySI) and [Viper](https://github.com/Khoury-srg/Viper) struggle to have comparable performance to our white-box tools. Installing and running them might be a time consuming work, so we don't provide automated scripts for running them. But we provide the data for all the tools. You can try them on a small history following the instructions on their homepages using our history data if you're interested.

Since Emme is not open source up to now, we implemented a version ourselves to check SI. As it costs much time and space to construct SSG to check SI on large histories, we don't provide automated scripts to run it in some experiments.

Installing and running [Cobra](https://github.com/DBCobra/CobraHome) for online checking requires a specific cuda version, so we don't provide the program. Data is also provided. Yon can try to install it yourself and run it with our data if you're interested.

For every experiment we provide `TimeKiller.jar` (and `elle-cli.jar` as well as `emme.py` if required) so you don't need to install it in advance. But you need Java 11 (or above) and Linux environment to run the scripts (some may require Python environment).

Data, programs and scripts can be downloaded from [Google Drive](https://drive.google.com/drive/folders/1JccrZqmm2L_rpJTjuuGLzXNDw_xu1Ycc).

### Fig 4

Running the scripts of this experiment requires `python3` (or you can modify `python3` to `python` in the scripts).

Moreover, you need `json` in your Python environment.

Download the `fig4.zip`, decompress it, enter `fig4/` directory, and run

```sh
./run-chronos.sh
./run-ellekv.sh
./run-emme.sh
```

Note that it may take some time for `elle-cli.jar` to load and exit, so your somatosensory running time may be longer than the actual running time. This applies to all the experiments that use `elle-cli.jar`.

### Fig 5

Download the `fig5.zip`, decompress it and enter `fig5/` directory.

To reproduce Figure 4a, run

```sh
./run-chronoskv.sh
./run-ellekv.sh
```

Please manually run `emme.py` on relatively smaller histories to see its results if you're interested.

To reproduce Figure 4b, run

```sh
./run-chronoslist.sh
./run-ellelist.sh
```

### Fig 6

Download the `fig6.zip`, decompress it and enter `fig6/` directory.

To reproduce subfigure a/b/c/d, run

```sh
./run-{a|b|c|d}.sh
```

### Fig 7

Running the scripts of this experiment requires `python3` (or you can modify `python3` to `python` in the scripts).

Download the `fig7.zip`, decompress it and enter `fig7/` directory.

To reproduce subfigure a or b, run

```sh
./run-{a|b}-chronos.sh
./run-{a|b}-ellekv.sh
```

Note that this experiment requires there are no other running `java` processes when a script is running. It also means that the scripts cannot be running at the same time.

Please manually run `emme.py` on relatively smaller histories and monitor its memory usage to see its results if you're interested.

### Fig 8

Download the `fig8.zip`, decompress it and enter `fig8/` directory.

To reproduce subfigure a or b, run

```sh
./run-{a|b}.sh
```

### Fig 9

Download the `fig9.zip`, decompress it, enter `fig9/` directory, and run

```sh
./run.sh
```

### Fig 10

Running this script requires `python3` (or you can modify it to `python` in the script), and this experiment also requires there are no other running `java` processes when the script is running.

Moreover, you need `numpy` and `matplotlib` in your Python environment.

Download the `fig10.zip`, decompress it, enter `fig10/` directory, and run

```sh
./run.sh
```

When the script finishes, you will get `gc-{10k,20k,50k,infinity}.png` in the `fig9/` directory depicting memory usage over time under different GC frequencies.

### Fig 12

Download the `fig12.zip`, decompress it and enter `fig12/` directory.

To calculate TPS of Aion, we record the timestamp when each transaction is checked for the first time, so the `TimeKiller-fig12.jar` for this experiment is a special edition. Please use the file in the `fig12/` directory. If you want to install it yourself, delete the comment `System.out.println(System.currentTimeMillis());` in the `runOnlineMode()` method in [TimeKiller.java](./src/main/java/TimeKiller.java) and `/ 1000` in the `preGc()` method in [SEROnlineChecker.java](./src/main/java/checker/ser/SEROnlineChecker.java) (to temporarily convert 16-digit YugabyteDB timestamps into 13-digit), and then [reinstall](#Install) it.

You need `json` and `requests` in your Python environment for this experiment. Also, this experiment requires a lot of memory, preferably 64GB or more.

To reproduce Figure 10a (no-gc strategy), first start Aion.

```sh
java -XX:+UseG1GC -Xmx48G -jar TimeKiller-fig12.jar --mode online --consistency_model SER  --use_cts_as_rtts true --txn_start_gc 550000 --max_txn_in_mem 550000 --gc_interval 5000 > a/no-gc.txt
```

Then start another session, run `send_request_ser.py` (to simulate requests of database transactions) to send requests to Aion.

```sh
python3 send_request_ser.py ./a/ser-500k.txt
```

When all requests are sent (i.e. the python script ends), you can check the TPS of Aion.

```sh
python3 aion_tps.py ./a/no-gc.txt 5000
```

You can check the TPS multiple times util the result remains unchanged.

Running Aion-SER under other two GC strategies is similar.

```sh
# checking-gc strategy
java -XX:+UseG1GC -Xmx48G -jar TimeKiller-fig12.jar --mode online --consistency_model SER  --use_cts_as_rtts true --txn_start_gc 200000 --max_txn_in_mem 550000 --gc_interval 5000 > a/checking-gc.txt
# start another session
python3 send_request_ser.py ./a/ser-500k.txt
# after the python script ends
python3 aion_tps.py ./a/checking-gc.txt 5000
# full-gc strategy
java -XX:+UseG1GC -Xmx48G -jar TimeKiller-fig12.jar --mode online --consistency_model SER  --use_cts_as_rtts true --txn_start_gc 200000 --max_txn_in_mem 430000 --gc_interval 5000 > a/full-gc.txt
# start another session
python3 send_request_ser.py ./a/ser-500k.txt
# after the python script ends
python3 aion_tps.py ./a/full-gc.txt 5000
```

The step to reproduce Figure 10b is the same as Figure 10a, except that the parameters are different.

```sh
# no-gc strategy
java -XX:+UseG1GC -Xmx48G -jar TimeKiller-fig12.jar --mode online --initial_value 0 --use_cts_as_rtts true --txn_start_gc 550000 --max_txn_in_mem 550000 --gc_interval 5000 > b/no-gc.txt
# start another session
python3 send_request_si.py ./b/si-500k.json
# after the python script ends
python3 aion_tps.py ./b/no-gc.txt 5000
# checking-gc strategy
java -XX:+UseG1GC -Xmx48G -jar TimeKiller-fig12.jar --mode online --initial_value 0 --use_cts_as_rtts true --txn_start_gc 200000 --max_txn_in_mem 550000 --gc_interval 5000 > b/checking-gc.txt
# start another session
python3 send_request_si.py ./b/si-500k.json
# after the python script ends
python3 aion_tps.py ./b/checking-gc.txt 5000
# full-gc strategy
java -XX:+UseG1GC -Xmx48G -jar TimeKiller-fig12.jar --mode online --initial_value 0 --use_cts_as_rtts true --txn_start_gc 200000 --max_txn_in_mem 450000 --gc_interval 5000 > b/full-gc.txt
# start another session
python3 send_request_si.py ./b/si-500k.json
# after the python script ends
python3 aion_tps.py ./b/full-gc.txt 5000
```

Other two subfigures are similar to the above steps.

```sh
java -XX:+UseG1GC -Xmx55G -jar TimeKiller-fig12.jar --mode online --initial_txn_path ./rubis-init.json --use_cts_as_rtts true --txn_start_gc 550000 --max_txn_in_mem 550000 --gc_interval 5000 --consistency_model SER > c/no-gc.txt
python3 send_request_rubis.py ./c/rubis-log.json
python3 aion_tps.py ./c/no-gc.txt 5000
java -XX:+UseG1GC -Xmx55G -jar TimeKiller-fig12.jar --mode online --initial_txn_path ./rubis-init.json --use_cts_as_rtts true --txn_start_gc 200000 --max_txn_in_mem 550000 --gc_interval 5000 --consistency_model SER > c/checking-gc.txt
python3 send_request_rubis.py ./c/rubis-log.json
python3 aion_tps.py ./c/checking-gc.txt 5000
java -XX:+UseG1GC -Xmx55G -jar TimeKiller-fig12.jar --mode online --initial_txn_path ./rubis-init.json --use_cts_as_rtts true --txn_start_gc 200000 --max_txn_in_mem 430000 --gc_interval 5000 --consistency_model SER > c/full-gc.txt
python3 send_request_rubis.py ./c/rubis-log.json
python3 aion_tps.py ./c/full-gc.txt 5000
```

```sh
java -XX:+UseG1GC -Xmx62G -jar TimeKiller-fig12.jar --mode online --initial_txn_path ./twitter-init.json --use_cts_as_rtts true --txn_start_gc 550000 --max_txn_in_mem 550000 --gc_interval 5000 --consistency_model SER > d/no-gc.txt
python3 send_request_twitter.py ./d/twitter-log.json
python3 aion_tps.py ./d/no-gc.txt 5000
java -XX:+UseG1GC -Xmx62G -jar TimeKiller-fig12.jar --mode online --initial_txn_path ./twitter-init.json --use_cts_as_rtts true --txn_start_gc 250000 --max_txn_in_mem 550000 --gc_interval 5000 --consistency_model SER > d/checking-gc.txt
python3 send_request_twitter.py ./d/twitter-log.json
python3 aion_tps.py ./d/checking-gc.txt 5000
java -XX:+UseG1GC -Xmx62G -jar TimeKiller-fig12.jar --mode online --initial_txn_path ./twitter-init.json --use_cts_as_rtts true --txn_start_gc 250000 --max_txn_in_mem 400000 --gc_interval 5000 --consistency_model SER > d/full-gc.txt
python3 send_request_twitter.py ./d/twitter-log.json
python3 aion_tps.py ./d/full-gc.txt 5000
```

### Fig 13-16

These experiments require a Dgraph instance, a workload generator, a runner to connect to Dgraph and run the workload, and a collector to collect the information of the committed transactions and send them to Aion. Please follow the instructions of run end-to-end first to run Dgraph and set up the environment for dbcdc-runner.

We provide a collector `collect-dgraph.py` as an example which also supports injecting normally distributed delays used in Figure 13 and 14.

```sh
python collect-dgraph.py <total_txn_num>
python collect-dgraph.py <total_txn_num> <mu> <sigma> <interval_between_txns>
```

Moreover, you need to modify the `commit-transaction-v2` function in `dgraph.clj` of the dbcdc-runner project, sending a request to the collector when a transaction commits.

To reproduce each of these experiments, first start Dgraph instance, Aion (just with default parameters) and collector, then start generator and runner until it finishes.

To reproduce Figure 13 and 14, you need to add `--log_ext_flip` to run Aion. We provide a script `count-flip.py` to parse the log and count the number of flip-flops and the time spent on rectifying the EXT violations.

To reproduce Figure 15, you need to record the time when the runner starts and ends executing the transaction to calculate the database throughput. To run without collecting history, don't send a transaction to collector with it commits.

To reproduce Figure 16, you need to monitor the memory usage of Aion when it's running.