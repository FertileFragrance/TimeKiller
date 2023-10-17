package reader;

import arg.Arg;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONReader;
import history.History;
import history.transaction.*;
import org.apache.commons.lang3.tuple.Pair;
import violation.SESSION;
import violation.Violation;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

public class JSONFileGcReader implements Reader<Long, Long> {
    @Override
    public Pair<History<Long, Long>, ArrayList<Violation>> read(String filepath) {
        ArrayList<TransactionEntry<Long, Long>> txnEntries = null;
        ArrayList<Violation> violations = new ArrayList<>();
        HashMap<String, Transaction<Long, Long>> lastInSession = new HashMap<>(41);
        long maxKey = Long.MIN_VALUE;
        try {
            JSONReader jsonReader = new JSONReader(new FileReader(filepath));
            JSONArray jsonArray = (JSONArray) jsonReader.readObject();
            int size = jsonArray.size();
            txnEntries = new ArrayList<>(2 * size + 2);
            txnEntries.add(new TransactionEntry<>(null, TransactionEntry.EntryType.START, null));
            txnEntries.add(new TransactionEntry<>(null, TransactionEntry.EntryType.COMMIT, null));
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
                txnEntries.add(new TransactionEntry<>(txn, TransactionEntry.EntryType.START, txn.getStartTimestamp()));
                txnEntries.add(new TransactionEntry<>(txn, TransactionEntry.EntryType.COMMIT, txn.getCommitTimestamp()));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        assert txnEntries != null;
        Pair<Transaction<Long, Long>, HashMap<Long, Transaction<Long, Long>>> initialTxnAndFrontier = createInitialTxn(maxKey);
        Transaction<Long, Long> initialTxn = initialTxnAndFrontier.getLeft();
        txnEntries.set(0, new TransactionEntry<>(initialTxn, TransactionEntry.EntryType.START,
                initialTxn.getStartTimestamp()));
        txnEntries.set(1, new TransactionEntry<>(initialTxn, TransactionEntry.EntryType.COMMIT,
                initialTxn.getCommitTimestamp()));
        return Pair.of(new History<>(null, initialTxn.getExtWriteKeys().size(),
                txnEntries, null, initialTxnAndFrontier.getRight()), violations);
    }

    @Override
    public int obtainFirstIndexToCheck() {
        return 0;
    }

    private Pair<Transaction<Long, Long>, HashMap<Long, Transaction<Long, Long>>> createInitialTxn(long maxKey) {
        HashMap<Long, Transaction<Long, Long>> frontier = new HashMap<>((int) (maxKey * 4 / 3 + 1));
        int opSize = (int) maxKey + 1;
        ArrayList<Operation<Long, Long>> operations = new ArrayList<>(opSize);
        HashMap<Long, Long> extWriteKeys = new HashMap<>(opSize * 4 / 3 + 1);
        HybridLogicalClock startTimestamp = new HybridLogicalClock(0L, 0L);
        HybridLogicalClock commitTimestamp = new HybridLogicalClock(0L, 0L);
        Transaction<Long, Long> initialTxn = new Transaction<>("initial", "initial",
                operations, startTimestamp, commitTimestamp);
        initialTxn.setExtWriteKeys(extWriteKeys);
        for (long key = 0; key <= maxKey; key++) {
            operations.add(new Operation<>(OpType.write, key, Arg.INITIAL_VALUE_LONG));
            extWriteKeys.put(key, Arg.INITIAL_VALUE_LONG);
            frontier.put(key, initialTxn);
        }
        return Pair.of(initialTxn, frontier);
    }
}
