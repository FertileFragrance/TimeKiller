package reader.si;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONReader;
import history.History;
import history.transaction.HybridLogicalClock;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;
import info.Arg;
import info.Stats;
import org.apache.commons.lang3.tuple.Pair;
import reader.Reader;
import violation.SESSION;
import violation.Violation;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

public class SIListFastReader implements Reader<Long, Long> {
    private Transaction<Long, Long> initialTxn;
    private HashMap<Long, ArrayList<Transaction<Long, Long>>> keyWritten;

    @Override
    public Pair<History<Long, Long>, ArrayList<Violation>> read(Object filepath) {
        Stats.LOADING_START = System.currentTimeMillis();

        ArrayList<Transaction<Long, Long>> txns = null;
        ArrayList<Violation> violations = new ArrayList<>();
        HashMap<String, Transaction<Long, Long>> lastInSession = new HashMap<>(41);
        long maxKey = Long.MIN_VALUE;
        long readOpCount = 0L;
        long writeOpCount = 0L;
        try {
            JSONReader jsonReader = new JSONReader(new FileReader((String) filepath));
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
                    if ("a".equalsIgnoreCase(type) || "append".equalsIgnoreCase(type)) {
                        Long value = jsonOperation.getLong("v");
                        Operation<Long, Long> op = new Operation<>(OpType.write, key, value);
                        ops.add(op);
                        writeOpCount++;
                    } else if ("r".equalsIgnoreCase(type) || "read".equalsIgnoreCase(type)) {
                        JSONArray jsonValues = jsonOperation.getJSONArray("v");
                        Long value;
                        if (jsonValues == null || jsonValues.isEmpty()) {
                            value = Arg.INITIAL_VALUE_LONG;
                        } else {
                            value = jsonValues.getLong(jsonValues.size() - 1);
                        }
                        Operation<Long, Long> op = new Operation<>(OpType.read, key, value);
                        ops.add(op);
                        readOpCount++;
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
        System.gc();
        assert txns != null;
        createInitialTxn(maxKey);
        txns.set(0, initialTxn);

        Stats.LOADING_END = System.currentTimeMillis();

        long opCount = readOpCount + writeOpCount;
        System.out.println("==========[ Txn Info Statistics ]==========");
        System.out.printf("|  Number of txns:          %-10d    |\n", txns.size() - 1);
        System.out.printf("|  Number of sessions:      %-10d    |\n", lastInSession.size());
        System.out.printf("|  Maximum key:             %-10d    |\n", maxKey);
        System.out.printf("|  Avg num of ops per txn:  %-10f    |\n", (double) opCount / (txns.size() - 1));
        System.out.printf("|  Read op percentage:      %-10f    |\n", (double) readOpCount / opCount);
        System.out.printf("|  Write op percentage:     %-10f    |\n", (double) writeOpCount / opCount);
        System.out.println("===========================================");

        return Pair.of(new History<>(txns, keyWritten.size(), null,
                keyWritten, null), violations);
    }

    private void createInitialTxn(long maxKey) {
        if (Arg.INITIAL_TXN_PATH != null) {
            createInitialTxnFromFile(maxKey);
            return;
        }
        keyWritten = new HashMap<>((int) (maxKey * 4 / 3 + 1));
        int opSize = (int) maxKey + 1;
        ArrayList<Operation<Long, Long>> operations = new ArrayList<>(opSize);
        HashMap<Long, Long> extWriteKeys = new HashMap<>(opSize * 4 / 3 + 1);
        HybridLogicalClock startTimestamp = new HybridLogicalClock(0L, 0L);
        HybridLogicalClock commitTimestamp = new HybridLogicalClock(0L, 0L);
        initialTxn = new Transaction<>("initial", "initial",
                operations, startTimestamp, commitTimestamp);
        initialTxn.setExtWriteKeys(extWriteKeys);
        for (long key = 0; key <= maxKey; key++) {
            operations.add(new Operation<>(OpType.write, key, Arg.INITIAL_VALUE_LONG));
            extWriteKeys.put(key, Arg.INITIAL_VALUE_LONG);
            ArrayList<Transaction<Long, Long>> writeToKeyTxns = new ArrayList<>(129);
            writeToKeyTxns.add(initialTxn);
            keyWritten.put(key, writeToKeyTxns);
        }
    }

    private void createInitialTxnFromFile(long maxKey) {
        keyWritten = new HashMap<>((int) (maxKey * 4 / 3 + 1));
        int opSize = (int) maxKey + 1;
        ArrayList<Operation<Long, Long>> operations = new ArrayList<>(opSize);
        HashMap<Long, Long> extWriteKeys = new HashMap<>(opSize * 4 / 3 + 1);
        HybridLogicalClock startTimestamp = new HybridLogicalClock(0L, 0L);
        HybridLogicalClock commitTimestamp = new HybridLogicalClock(0L, 0L);
        initialTxn = new Transaction<>("initial", "initial",
                operations, startTimestamp, commitTimestamp);
        initialTxn.setExtWriteKeys(extWriteKeys);
        try {
            JSONReader jsonReader = new JSONReader(new FileReader(Arg.INITIAL_TXN_PATH));
            JSONObject jsonObject = (JSONObject) jsonReader.readObject();
            JSONArray jsonOps = jsonObject.getJSONArray("ops");
            for (Object objOp : jsonOps) {
                JSONObject jsonOperation = (JSONObject) objOp;
                String type = jsonOperation.getString("t");
                Long key = jsonOperation.getLong("k");
                maxKey = Math.max(maxKey, key);
                if ("a".equalsIgnoreCase(type) || "append".equalsIgnoreCase(type)) {
                    Long value = jsonOperation.getLong("v");
                    operations.add(new Operation<>(OpType.write, key, value));
                    extWriteKeys.put(key, value);
                    ArrayList<Transaction<Long, Long>> writeToKeyTxns = new ArrayList<>(129);
                    writeToKeyTxns.add(initialTxn);
                    keyWritten.put(key, writeToKeyTxns);
                } else if ("r".equalsIgnoreCase(type) || "read".equalsIgnoreCase(type)) {
                    JSONArray jsonValues = jsonOperation.getJSONArray("v");
                    Long value;
                    if (jsonValues == null || jsonValues.isEmpty()) {
                        value = Arg.INITIAL_VALUE_LONG;
                    } else {
                        value = jsonValues.getLong(jsonValues.size() - 1);
                    }
                    operations.add(new Operation<>(OpType.read, key, value));
                } else {
                    throw new RuntimeException("Unknown operation type.");
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        for (long key = 0; key <= maxKey; key++) {
            if (!extWriteKeys.containsKey(key)) {
                operations.add(new Operation<>(OpType.write, key, Arg.INITIAL_VALUE_LONG));
                extWriteKeys.put(key, Arg.INITIAL_VALUE_LONG);
                ArrayList<Transaction<Long, Long>> writeToKeyTxns = new ArrayList<>(129);
                writeToKeyTxns.add(initialTxn);
                keyWritten.put(key, writeToKeyTxns);
            }
        }
    }
}
