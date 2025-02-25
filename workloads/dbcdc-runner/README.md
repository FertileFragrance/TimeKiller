# DB-CDC Runner

This project is an execution framework for running CDC-based Jepsen tests. The framework is based on the tests Jepsen performed on PostgreSQL [stolon](https://github.com/jepsen-io/jepsen/tree/main/stolon).

## How to run? Take Dgraph as an example

### Step 1: Install dependencies
Please refer to [Jepsen's repository](https://github.com/jepsen-io/jepsen) for the installation of Jepsen.

### Step 2: Configure the database
We provide a template file located at `resources/db-config-template.edn`. Rename it to `db-config.edn` and fill in the data source parameters.

If you have started a Dgraph instance, you can use the following configuration:
```clojure
{:dgraph     {:dbyte     "dgraph"
              :host      "localhost"
              :port      9080}}
```

### Step 3: Generate a workload
Instead of using Jepsen's workload generator, we use DBCOP to generate the workload. We have modified the DBCOP generator to generate the workload in the format required by this project. Please see the [repository](https://anonymous.4open.science/r/dbcop-6369) and install it first.

Generate a workload in the bincode format:
```bash
dbcop generate -d /tmp/generate/ -n 25 -t 100 -e 20 -v 1000 --nhist 1
```

Then convert the bincode format to the JSON format:
```bash
dbcop convert -d /tmp/generate --from bincode
```

You will get `/tmp/generate/hist-00000.json`.

### Step 4: Run a test
Use the following command to run a test on Dgraph:
```bash
lein run test-all -w rw \
--txn-num 120000 \
--time-limit 43200 \
-r 10000 \
--node dummy-node \
--isolation snapshot-isolation \
--expected-consistency-model snapshot-isolation \
--nemesis none \
--existing-postgres \
--no-ssh \
--database dgraph \
--dbcop-workload-path /tmp/generate/hist-00000.json \
--dbcop-workload
```

When it is finished, you can see the result in the `./store/lastest/` directory.

Run the following command to see the help information (some of the parameters don't have any effect when using DBCOP as the workload generator):
``` bash
lein run test --help
```

### Step 5: Convert format
There is a file `./store/lastest/history.edn` that contains the information of transactions. We provide a script to convert the edn format to the JSON format required by Chronos and Aion. Run the following command:
```bash
python3 edn2json.py ./store/lastest/history.edn
```

Then you will get `./dgraph_history.json`. The easiest way to use this file is to run Chronos with `--history_path /path/to/dgraph_history.json --initial_value 0`.
