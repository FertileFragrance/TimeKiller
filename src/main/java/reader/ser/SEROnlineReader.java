package reader.ser;

import checker.online.GcUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import history.History;
import history.transaction.*;
import info.Arg;
import org.apache.commons.lang3.tuple.Pair;
import reader.Reader;
import violation.SESSION;
import violation.TRANSVIS;
import violation.Violation;

import java.util.ArrayList;
import java.util.HashMap;

public class SEROnlineReader implements Reader<Long, Long> {
    private History<Long, Long> history;
    private final HashMap<String, Transaction<Long, Long>> lastInSession = new HashMap<>(41);
    private long maxKey = 1000;

    @Override
    public Pair<History<Long, Long>, ArrayList<Violation>> read(Object jsonObj) {
        ArrayList<Transaction<Long, Long>> txns;
        ArrayList<Violation> violations = new ArrayList<>();
        if (history == null) {
            txns = new ArrayList<>(10001);
            Transaction<Long, Long> initialTxn = createInitialTxn();
            txns.add(initialTxn);
            history = new History<>(txns, initialTxn.getExtWriteKeys().size(),
                    null, null, null);
            history.setCommitEntryIndex(0);
            history.setCommitEntryIndexInMemory(0);
            return Pair.of(history, violations);
        }
        long lastMaxKey = maxKey;
        txns = history.getTransactions();
        ArrayList<Pair<String, Boolean>> tidEntryWhetherGc = history.getTidEntryWhetherGc();
        JSONObject jsonObject = (JSONObject) jsonObj;
        String sid = jsonObject.getString("sid");
        String txnId = jsonObject.getString("tid");
        JSONObject jsonStartTs = jsonObject.getJSONObject("sts");
        HybridLogicalClock startTs = new HybridLogicalClock(jsonStartTs.getLong("p"), jsonStartTs.getLong("l"));
        JSONObject jsonCommitTs = jsonObject.getJSONObject("cts");
        HybridLogicalClock commitTs = new HybridLogicalClock(jsonCommitTs.getLong("p"), jsonCommitTs.getLong("l"));
        Long realtimeTs;
        if (Arg.USE_CTS_AS_RTTS) {
            realtimeTs = commitTs.getPhysical();
        } else {
            realtimeTs = jsonObject.getLong("rtts");
        }
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
        txn.setRealtimeTimestamp(realtimeTs);
        if (Arg.ENABLE_SESSION && lastInSession.containsKey(sid) &&
                lastInSession.get(sid).getCommitTimestamp().compareTo(txn.getStartTimestamp()) > 0) {
            violations.add(new SESSION<>(lastInSession.get(sid), txn, sid));
        }
        lastInSession.put(sid, txn);
        // insertion sort
        int inMemoryIndex = txns.size() - 1;
        for (int j = tidEntryWhetherGc.size() - 1; j >= 0; j--) {
            Transaction<Long, Long> existingTxn;
            if (!tidEntryWhetherGc.get(j).getRight()) {
                existingTxn = txns.get(inMemoryIndex);
            } else {
                existingTxn = GcUtil.readTxn(tidEntryWhetherGc.get(j).getLeft());
            }
            assert existingTxn != null;
            if (commitTs.compareTo(existingTxn.getCommitTimestamp()) >= 0) {
                tidEntryWhetherGc.add(j + 1, Pair.of(txnId, false));
                txns.add(inMemoryIndex + 1, txn);
                history.setCommitEntryIndex(j + 1);
                history.setCommitEntryIndexInMemory(inMemoryIndex + 1);
//                // check TRANSVIS
//                if (startTs.compareTo(existingTxn.getCommitTimestamp()) < 0) {
//                    violations.add(new TRANSVIS<>(existingTxn, txn));
//                }
                break;
            }
            if (!tidEntryWhetherGc.get(j).getRight()) {
                inMemoryIndex--;
            }
        }
        updateInitialTxn(lastMaxKey, txns.get(0));
        return Pair.of(history, violations);
    }

    private Transaction<Long, Long> createInitialTxn() {
        HashMap<Long, TidVal<Long>> commitFrontierTidVal = new HashMap<>((int) (maxKey * 4 / 3 + 1));
        int opSize = (int) maxKey + 1;
        ArrayList<Operation<Long, Long>> operations = new ArrayList<>(opSize);
        HashMap<Long, Long> extWriteKeys = new HashMap<>(opSize * 4 / 3 + 1);
        HybridLogicalClock startTimestamp = new HybridLogicalClock(0L, 0L);
        HybridLogicalClock commitTimestamp = new HybridLogicalClock(0L, 0L);
        Transaction<Long, Long> initialTxn = new Transaction<>("initial", "initial",
                operations, startTimestamp, commitTimestamp);
        initialTxn.setExtWriteKeys(extWriteKeys);
        initialTxn.setCommitFrontierTidVal(commitFrontierTidVal);
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
