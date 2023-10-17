package reader;

import arg.Arg;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONArray;
import history.History;
import history.transaction.HybridLogicalClock;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;
import org.apache.commons.lang3.tuple.Pair;
import violation.SESSION;
import violation.Violation;

import java.util.ArrayList;
import java.util.HashMap;

public class OnlineReader implements Reader<Long, Long> {
    private History<Long, Long> history;
    private final HashMap<String, Transaction<Long, Long>> lastInSession = new HashMap<>(41);
    private long maxKey = Long.MIN_VALUE;
    private int firstIndex = 0;

    @Override
    public Pair<History<Long, Long>, ArrayList<Violation>> read(String jsonString) {
        ArrayList<Transaction<Long, Long>> txns;
        ArrayList<Violation> violations = new ArrayList<>();
        long lastMaxKey = maxKey;
        JSONArray jsonArray = JSON.parseArray(jsonString);
        int size = jsonArray.size();
        if (history == null) {
            txns = new ArrayList<>(size + 1);
            txns.add(new Transaction<>(null, null, null, null, null));
        } else {
            txns = history.getTransactions();
            firstIndex = txns.size();
        }
        for (int i = 0; i < size; i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String sid = jsonObject.getString("sid");
            String txnId = jsonObject.getString("tid");
            JSONObject jsonStartTs = jsonObject.getJSONObject("sts");
            HybridLogicalClock startTs = new HybridLogicalClock(jsonStartTs.getLong("p"), jsonStartTs.getLong("l"));
            JSONObject jsonCommitTs = jsonObject.getJSONObject("cts");
            HybridLogicalClock commitTs = new HybridLogicalClock(jsonCommitTs.getLong("p"), jsonCommitTs.getLong("l"));
            if (startTs.compareTo(commitTs) > 0) {
                throw new RuntimeException("!!! start_ts > commit_ts !!!");
            }
            JSONArray jsonOps = jsonObject.getJSONArray("ops");
            ArrayList<Operation<Long, Long>> ops = new ArrayList<>(jsonOps.size());
            for (Object objOp : jsonOps) {
                JSONObject jsonOperation = (JSONObject) objOp;
                String type = jsonOperation.getString("t");
                Long key = jsonOperation.getLong("k");
                maxKey = Math.max(maxKey, key);
                Long value = jsonOperation.getLong("v");
                if ("w".equalsIgnoreCase(type) || "write".equalsIgnoreCase(type)) {
                    Operation<Long, Long> op = new Operation<>(OpType.write, key, value);
                    ops.add(op);
                } else if ("r".equalsIgnoreCase(type) || "read".equalsIgnoreCase(type)) {
                    Operation<Long, Long> op = new Operation<>(OpType.read, key, value);
                    ops.add(op);
                } else {
                    throw new RuntimeException("Unknown operation type.");
                }
            }
            Transaction<Long, Long> txn = new Transaction<>(txnId, sid, ops, startTs, commitTs);
            if (Arg.ENABLE_SESSION && lastInSession.containsKey(sid) &&
                    lastInSession.get(sid).getCommitTimestamp().compareTo(txn.getStartTimestamp()) > 0) {
                violations.add(new SESSION<>(lastInSession.get(sid), txn, sid));
            }
            lastInSession.put(sid, txn);
            if (history == null) {
                txns.add(txn);
            } else {
                for (int j = txns.size() - 1; j >= 0; j--) {
                    Transaction<Long, Long> existingTxn = txns.get(j);
                    if (txn.getCommitTimestamp().compareTo(existingTxn.getCommitTimestamp()) >= 0) {
                        txns.add(j + 1, txn);
                        firstIndex = Math.min(firstIndex, j + 1);
                        break;
                    }
                }
            }
        }
        if (history == null) {
            Pair<Transaction<Long, Long>, HashMap<Long, ArrayList<Transaction<Long, Long>>>> initialTxnAndKeyWritten = createInitialTxn();
            txns.set(0, initialTxnAndKeyWritten.getLeft());
            history = new History<>(txns, initialTxnAndKeyWritten.getRight().size(),
                    null, initialTxnAndKeyWritten.getRight(), null);
        } else {
            updateInitialTxn(lastMaxKey, txns.get(0));
        }
        return Pair.of(history, violations);
    }

    @Override
    public int obtainFirstIndexToCheck() {
        return firstIndex;
    }

    private Pair<Transaction<Long, Long>, HashMap<Long, ArrayList<Transaction<Long, Long>>>> createInitialTxn() {
        HashMap<Long, ArrayList<Transaction<Long, Long>>> keyWritten = new HashMap<>((int) (maxKey * 4 / 3 + 1));
        int opSize = (int) maxKey + 1;
        ArrayList<Operation<Long, Long>> operations = new ArrayList<>(opSize);
        HashMap<Long, Long> extWriteKeys = new HashMap<>(opSize * 4 / 3 + 1);
        HybridLogicalClock startTimestamp = new HybridLogicalClock(0L, 0L);
        HybridLogicalClock commitTimestamp = new HybridLogicalClock(0L, 0L);
        Transaction<Long, Long> initialTxn = new Transaction<>("initial", "initial",
                operations, startTimestamp, commitTimestamp);
        initialTxn.setExtWriteKeys(extWriteKeys);
        for (long key = 0; key <= maxKey; key++) {
            operations.add(new Operation<>(OpType.write, key, null));
            extWriteKeys.put(key, null);
            ArrayList<Transaction<Long, Long>> writeToKeyTxns = new ArrayList<>(129);
            writeToKeyTxns.add(initialTxn);
            keyWritten.put(key, writeToKeyTxns);
        }
        return Pair.of(initialTxn, keyWritten);
    }

    private void updateInitialTxn(long lastMaxKey, Transaction<Long, Long> initialTxn) {
        if (maxKey == lastMaxKey) {
            return;
        }
        HashMap<Long, ArrayList<Transaction<Long, Long>>> keyWritten = history.getKeyWritten();
        ArrayList<Operation<Long, Long>> operations = initialTxn.getOperations();
        HashMap<Long, Long> extWriteKeys = initialTxn.getExtWriteKeys();
        for (long key = lastMaxKey + 1; key <= maxKey; key++) {
            operations.add(new Operation<>(OpType.write, key, Arg.INITIAL_VALUE_LONG));
            extWriteKeys.put(key, Arg.INITIAL_VALUE_LONG);
            ArrayList<Transaction<Long, Long>> writeToKeyTxns = new ArrayList<>(129);
            writeToKeyTxns.add(initialTxn);
            keyWritten.put(key, writeToKeyTxns);
        }
    }
}
