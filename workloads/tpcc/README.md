# TPCC Workload Runner

## Prepare
Before running the TPCC workload, please start a Dgraph instance first.

Edit the constants in `load_data.py` and `tpcc-dgraph.py` to fit your needs, and make the IP address and port (the `create_client_stub` function in `dgraph_impl.py` and the `connect_to_dgraph` function in `tpcc-dgraph.py`) match your Dgraph instance.

## Run
First, load some data into Dgraph:
```bash
python3 load_data.py > load.txt
```

The file `load.txt` contains the results of data loading, which will be processed as the initial transaction later.

Then, run the TPCC workload:
```bash
python3 tpcc-dgraph.py > run.txt
```

When the workload finishes, you will get the results in `run.txt`.

Note that if you write the output to some other files, you need to modify `tpcc-convert.py` to read the correct files.

## Convert
Use the following command to convert the results to the JSON format required by Chronos and Aion:
```bash
python3 tpcc-convert.py
```

Then you will get `./tpcc-init.json` and `./tpcc-log.json`. The former is the customized initial transaction, and the latter contains the transactions to be checked.

The easiest way to use these files is to run Chronos with `--history_path /path/to/tpcc-log.json --initial_txn_path /path/to/tpcc-init.json`.
