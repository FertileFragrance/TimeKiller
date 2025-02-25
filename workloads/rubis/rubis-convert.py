import json

CONSTANT_USER_NUM = 200
CONSTANT_ITEM_NUM_PER_USER = 4
CONSTANT_THREAD_NUM = 24


def main():
    count = 0
    txns = []
    for i in range(CONSTANT_THREAD_NUM + 1):
        with open('./rubis-log/' + str(i) + '.json', 'r') as f:
            thread_txns = json.load(f)
        for txn in thread_txns:
            txn['tid'] = count
            count += 1
            for op in txn['ops']:
                table_and_k = op['k']
                table, k = table_and_k.split(':')
                k = int(k)
                if table == 'rating':
                    real_k = k
                else:
                    real_k = k + CONSTANT_USER_NUM
                op['k'] = real_k
            txns.append(txn)
    txns.sort(key=lambda x: x['mts'])
    for txn in txns:
        del txn['mts']
    initial_txn = txns[0]
    with open('./rubis-init.json', 'w') as f:
        json.dump(initial_txn, f)
    txns.pop(0)
    with open('./rubis-log.json', 'w') as f:
        json.dump(txns, f)


if __name__ == '__main__':
    main()
