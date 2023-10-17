package reader;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONReader;
import history.History;
import history.transaction.HybridLogicalClock;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;
import violation.SESSION;
import violation.Violation;
import arg.Arg;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

public class JSONFileFastReader implements Reader<Long, Long> {
    @Override
    public Pair<History<Long, Long>, ArrayList<Violation>> read(String filepath) {
        ArrayList<Transaction<Long, Long>> txns = null;
        ArrayList<Violation> violations = new ArrayList<>();
        HashMap<String, Transaction<Long, Long>> lastInSession = new HashMap<>(41);
        long maxKey = Long.MIN_VALUE;
        try {
            JSONReader jsonReader = new JSONReader(new FileReader(filepath));
            JSONArray jsonArray = (JSONArray) jsonReader.readObject();
            int size = jsonArray.size();
            txns = new ArrayList<>(size + 1);
            txns.add(new Transaction<>(null, null, null, null, null));
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
                txns.add(txn);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        assert txns != null;
        Pair<Transaction<Long, Long>, HashMap<Long, ArrayList<Transaction<Long, Long>>>> initialTxnAndKeyWritten = createInitialTxn(maxKey);
        txns.set(0, initialTxnAndKeyWritten.getLeft());
        return Pair.of(new History<>(txns, initialTxnAndKeyWritten.getRight().size(),
                null, initialTxnAndKeyWritten.getRight(), null), violations);
    }

    @Override
    public int obtainFirstIndexToCheck() {
        return 0;
    }

    private Pair<Transaction<Long, Long>, HashMap<Long, ArrayList<Transaction<Long, Long>>>> createInitialTxn(long maxKey) {
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
            operations.add(new Operation<>(OpType.write, key, Arg.INITIAL_VALUE_LONG));
            extWriteKeys.put(key, Arg.INITIAL_VALUE_LONG);
            ArrayList<Transaction<Long, Long>> writeToKeyTxns = new ArrayList<>(129);
            writeToKeyTxns.add(initialTxn);
            keyWritten.put(key, writeToKeyTxns);
        }
        return Pair.of(initialTxn, keyWritten);
    }
}
