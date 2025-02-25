# Twitter Workload Runner

## Prepare
Before running the Twitter workload, please start a Dgraph instance first.

Edit the constants in `twitter.py` and `twitter-convert.py` to fit your needs, and make the IP address and port (the `create_client_stub` function in `twitter.py`) match your Dgraph instance.

## Run
Use the following command to run the Twitter workload:
```bash
python3 twitter.py
```

When it is finished, you can see the results in `./twitter-log/`. Each file records the transactions of a session.

## Convert
Use the following command to convert the results to the JSON format required by Chronos and Aion:
```bash
python3 twitter-convert.py
```

Then you will get `./twitter-init.json` and `./twitter-log.json`. The former is the customized initial transaction, and the latter contains the transactions to be checked.

The easiest way to use these files is to run Chronos with `--history_path /path/to/twitter-log.json --initial_txn_path /path/to/twitter-init.json`.
