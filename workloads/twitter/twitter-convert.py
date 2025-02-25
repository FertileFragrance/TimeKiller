import json

CONSTANT_USER_NUM = 500
CONSTANT_TWEET_NUM_PER_USER = 1
CONSTANT_THREAD_NUM = 24


def main():
    count = 0
    txns = []
    for i in range(CONSTANT_THREAD_NUM + 1):
        with open('./twitter-log/' + str(i) + '.json', 'r') as f:
            thread_txns = json.load(f)
        for txn in thread_txns:
            txn['tid'] = count
            count += 1
            for op in txn['ops']:
                table_and_k = op['k']
                table, k = table_and_k.split(':')
                k = int(k)
                real_k = -1
                if table == 'last_tweet':
                    real_k = k
                elif table == 'follow_list':
                    real_k = k + CONSTANT_USER_NUM
                elif table == 'tweet':
                    real_k = k + CONSTANT_USER_NUM * 2
                op['k'] = real_k
                if table != 'last_tweet':
                    op['v'] = hash(op['v'])
            txns.append(txn)
    txns.sort(key=lambda x: x['mts'])
    for txn in txns:
        del txn['mts']
    initial_txn = txns[0]
    with open('./twitter-init.json', 'w') as f:
        json.dump(initial_txn, f)
    txns.pop(0)
    with open('./twitter-log.json', 'w') as f:
        json.dump(txns, f)


if __name__ == '__main__':
    main()
