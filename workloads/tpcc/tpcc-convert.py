import json

k_str2int = {}
curr_k = 0
txns = []
txn_count = 0


def parse(filename):
    global k_str2int, curr_k, txns, txn_count
    for line in open(filename, 'r'):
        if len(line) == 0 or line[0] != '{':
            continue
        line = line.strip().replace("\'", "\"")
        txn = json.loads(line)
        txn['tid'] = txn_count
        txn_count += 1
        for i in range(len(txn['ops'])):
            table_and_k = txn['ops'][i]['k']
            if table_and_k not in k_str2int:
                k_str2int[table_and_k] = curr_k
                curr_k += 1
            txn['ops'][i]['k'] = k_str2int[table_and_k]
            txn['ops'][i]['v'] = hash(txn['ops'][i]['v'])
        txns.append(txn)


def main():
    global txns, txn_count
    parse('./load.txt')
    t0 = {
        'tid': 0,
        'sid': 0,
        'sts': {'p': 0, 'l': 0},
        'cts': {'p': 0, 'l': 0},
        'ops': []
    }
    for txn in txns:
        t0['ops'].extend(txn['ops'])
    with open('./tpcc-init.json', 'w') as f:
        json.dump(t0, f)
    txns = []
    txn_count = 1
    parse('./run.txt')
    with open('./tpcc-log.json', 'w') as f:
        json.dump(txns, f)


if __name__ == '__main__':
    main()
