import edn_format
import json
import sys
from edn_format import Keyword


if __name__ == '__main__':
    filepath = sys.argv[1]
    txns = []
    with open(filepath, 'r') as f:
        for line in f:
            raw_txn = edn_format.loads(line)
            if raw_txn.get(Keyword('type')) == Keyword('invoke'):
                continue
            tid = raw_txn.get(Keyword('index'))
            sid = raw_txn.get(Keyword('process'))
            sts = {'p': raw_txn.get(Keyword('ts')).get(Keyword('rts')), 'l': 0}
            cts = {'p': raw_txn.get(Keyword('ts')).get(Keyword('cts')), 'l': 0}
            if raw_txn.get(Keyword('ts')).get(Keyword('cts')) is None:
                cts = {'p': raw_txn.get(Keyword('ts')).get(Keyword('rts')), 'l': 0}
            ops = []
            for op in raw_txn.get(Keyword('value')):
                t = 'r' if op[0] == Keyword('r') else 'w'
                k = op[1]
                v = op[2]
                ops.append({'t': t, 'k': k, 'v': v})
            txns.append({
                'tid': tid,
                'sid': sid,
                'sts': sts,
                'cts': cts,
                'ops': ops
            })
    json_txns = json.dumps(txns)
    with open('./dgraph_history.json', 'w') as f:
        f.write(json_txns)
