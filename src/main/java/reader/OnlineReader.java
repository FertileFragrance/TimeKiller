package reader;

import history.transaction.*;
import info.Arg;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONArray;
import history.History;
import org.apache.commons.lang3.tuple.Pair;
import violation.NOCONFLICT;
import violation.SESSION;
import violation.Violation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class OnlineReader implements Reader<Long, Long> {
    private History<Long, Long> history;
    private final HashMap<String, Transaction<Long, Long>> lastInSession = new HashMap<>(41);
    private long maxKey = 1000;

    @Override
    public Pair<History<Long, Long>, ArrayList<Violation>> read(Object jsonObj) {
        ArrayList<TransactionEntry<Long, Long>> txnEntries;
        ArrayList<Violation> violations = new ArrayList<>();
        if (history == null) {
            txnEntries = new ArrayList<>(2 * 10000 + 2);
            Transaction<Long, Long> initialTxn = createInitialTxn();
            txnEntries.add(new TransactionEntry<>(initialTxn, TransactionEntry.EntryType.START,
                    initialTxn.getStartTimestamp()));
            txnEntries.add(new TransactionEntry<>(initialTxn, TransactionEntry.EntryType.COMMIT,
                    initialTxn.getCommitTimestamp()));
            history = new History<>(null, initialTxn.getExtWriteKeys().size(),
                    txnEntries, null, null);
            return Pair.of(history, violations);
        }
        long lastMaxKey = maxKey;
        txnEntries = history.getTransactionEntries();
        JSONObject jsonObject = (JSONObject) jsonObj;
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
        HashSet<Long> writeKeys = new HashSet<>(jsonOps.size() * 4 / 3 + 1);
        for (Object objOp : jsonOps) {
            JSONObject jsonOperation = (JSONObject) objOp;
            String type = jsonOperation.getString("t");
            Long key = jsonOperation.getLong("k");
            maxKey = Math.max(maxKey, key);
            Long value = jsonOperation.getLong("v");
            if ("w".equalsIgnoreCase(type) || "write".equalsIgnoreCase(type)) {
                Operation<Long, Long> op = new Operation<>(OpType.write, key, value);
                ops.add(op);
                writeKeys.add(key);
            } else if ("r".equalsIgnoreCase(type) || "read".equalsIgnoreCase(type)) {
                Operation<Long, Long> op = new Operation<>(OpType.read, key, value);
                ops.add(op);
            } else {
                throw new RuntimeException("Unknown operation type.");
            }
        }
        Transaction<Long, Long> txn = new Transaction<>(txnId, sid, ops, startTs, commitTs);
        boolean violateSession = false;
        if (Arg.ENABLE_SESSION && lastInSession.containsKey(sid) &&
                lastInSession.get(sid).getCommitTimestamp().compareTo(txn.getStartTimestamp()) > 0) {
            violations.add(new SESSION<>(lastInSession.get(sid), txn, sid));
            violateSession = true;
        }
        lastInSession.put(sid, txn);
        // insertion sort
        TransactionEntry<Long, Long> startEntry =
                new TransactionEntry<>(txn, TransactionEntry.EntryType.START, startTs);
        TransactionEntry<Long, Long> commitEntry =
                new TransactionEntry<>(txn, TransactionEntry.EntryType.COMMIT, commitTs);
        HashSet<Transaction<Long, Long>> checkedTxns = new HashSet<>();
        boolean commitAdded = false;
        for (int j = txnEntries.size() - 1; j >= 0; j--) {
            TransactionEntry<Long, Long> existingEntry = txnEntries.get(j);
            if (commitTs.compareTo(existingEntry.getTimestamp()) >= 0 && !commitAdded) {
                txnEntries.add(j + 1, commitEntry);
                commitAdded = true;
                history.setCommitEntryIndex(j + 2);
            }
            // check NOCONFLICT
            if (startTs.compareTo(existingEntry.getTimestamp()) <= 0
                    && commitTs.compareTo(existingEntry.getTimestamp()) >= 0
                    && !checkedTxns.contains(existingEntry.getTransaction())) {
                for (Long k : existingEntry.getTransaction().getExtWriteKeys().keySet()) {
                    if (writeKeys.contains(k)) {
                        if (violateSession) {
                            violations.add(1, new NOCONFLICT<>(existingEntry.getTransaction(), txn, k));
                        } else {
                            violations.add(0, new NOCONFLICT<>(existingEntry.getTransaction(), txn, k));
                        }
                    }
                }
                checkedTxns.add(existingEntry.getTransaction());
            }
            if (startTs.compareTo(existingEntry.getTimestamp()) >= 0) {
                txnEntries.add(j + 1, startEntry);
                history.setStartEntryIndex(j + 1);
                break;
            }
        }
        updateInitialTxn(lastMaxKey, txnEntries.get(0).getTransaction());
        return Pair.of(history, violations);
    }

    private Transaction<Long, Long> createInitialTxn() {
        HashMap<Long, Transaction<Long, Long>> commitFrontier = new HashMap<>((int) (maxKey * 4 / 3 + 1));
        int opSize = (int) maxKey + 1;
        ArrayList<Operation<Long, Long>> operations = new ArrayList<>(opSize);
        HashMap<Long, Long> extWriteKeys = new HashMap<>(opSize * 4 / 3 + 1);
        HybridLogicalClock startTimestamp = new HybridLogicalClock(0L, 0L);
        HybridLogicalClock commitTimestamp = new HybridLogicalClock(0L, 0L);
        Transaction<Long, Long> initialTxn = new Transaction<>("initial", "initial",
                operations, startTimestamp, commitTimestamp);
        initialTxn.setExtWriteKeys(extWriteKeys);
        initialTxn.setCommitFrontier(commitFrontier);
        for (long key = 0; key <= maxKey; key++) {
            operations.add(new Operation<>(OpType.write, key, Arg.INITIAL_VALUE_LONG));
            extWriteKeys.put(key, Arg.INITIAL_VALUE_LONG);
        }
        return initialTxn;
    }

    private void updateInitialTxn(long lastMaxKey, Transaction<Long, Long> initialTxn) {
        if (maxKey == lastMaxKey) {
            return;
        }
        ArrayList<Operation<Long, Long>> operations = initialTxn.getOperations();
        HashMap<Long, Long> extWriteKeys = initialTxn.getExtWriteKeys();
        for (long key = lastMaxKey + 1; key <= maxKey; key++) {
            operations.add(new Operation<>(OpType.write, key, Arg.INITIAL_VALUE_LONG));
            extWriteKeys.put(key, Arg.INITIAL_VALUE_LONG);
        }
    }
}
