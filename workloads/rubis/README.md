# RUBiS Workload Runner

## Prepare
Before running the RUBiS workload, please start a Dgraph instance first.

Edit the constants in `rubis.py` and `rubis-convert.py` to fit your needs, and make the IP address and port (the `create_client_stub` function in `rubis.py`) match your Dgraph instance.

## Run
Use the following command to run the RUBiS workload:
```bash
python3 rubis.py
```

When it is finished, you can see the results in `./rubis-log/`. Each file records the transactions of a session.

## Convert
Use the following command to convert the results to the JSON format required by Chronos and Aion:
```bash
python3 rubis-convert.py
```

Then you will get `./rubis-init.json` and `./rubis-log.json`. The former is the customized initial transaction, and the latter contains the transactions to be checked.

The easiest way to use these files is to run Chronos with `--history_path /path/to/rubis-log.json --initial_txn_path /path/to/rubis-init.json`.
