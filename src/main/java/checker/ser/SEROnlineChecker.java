package checker.ser;

import checker.OnlineChecker;
import checker.online.GcUtil;
import checker.online.MarkTimeoutTask;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import history.History;
import history.transaction.*;
import info.Arg;
import org.apache.commons.lang3.tuple.Pair;
import violation.EXT;
import violation.INT;
import violation.Violation;
import violation.ViolationType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public class SEROnlineChecker implements OnlineChecker {
    private final Timer timer = new Timer();

    private final String cacheDir = System.getProperty("user.dir") + "/.cache/TimeKiller/";

    private final Kryo serializeKryo;

    public SEROnlineChecker() {
        serializeKryo = new Kryo();
        serializeKryo.register(HashSet.class);
        serializeKryo.register(Transaction.class);
        serializeKryo.register(HybridLogicalClock.class);
        serializeKryo.register(Operation.class);
        serializeKryo.register(OpType.class);
        serializeKryo.register(HashMap.class);
        serializeKryo.register(ArrayList.class);
        serializeKryo.register(EXT.class);
        serializeKryo.register(EXT.EXTType.class);
        serializeKryo.register(ViolationType.class);
        serializeKryo.register(TidVal.class);
        serializeKryo.setReferences(true);
    }

    @Override
    public <KeyType, ValueType> ArrayList<Violation> check(History<KeyType, ValueType> history) {
        ArrayList<Violation> violations = new ArrayList<>();
        Transaction<KeyType, ValueType> initialTxn = history.getInitialTxn();
        ArrayList<Transaction<KeyType, ValueType>> txns = history.getTransactions();
        ArrayList<Pair<String, Boolean>> tidEntryWhetherGc = history.getTidEntryWhetherGc();
        Transaction<KeyType, ValueType> currentTxn = txns.get(history.getCommitEntryIndexInMemory());
        Transaction<KeyType, ValueType> lastCommittedTxn;
        if (!tidEntryWhetherGc.get(history.getCommitEntryIndex() - 1).getRight()) {
            lastCommittedTxn = txns.get(history.getCommitEntryIndexInMemory() - 1);
        } else {
            lastCommittedTxn = GcUtil.readTxn(tidEntryWhetherGc.get(history.getCommitEntryIndex() - 1).getLeft());
        }
        assert lastCommittedTxn != null;
        int opSize = currentTxn.getOperations().size();
        HashMap<KeyType, ValueType> intKeys = new HashMap<>(opSize * 4 / 3 + 1);
        HashMap<KeyType, ValueType> extWriteKeys = new HashMap<>(opSize * 4 / 3 + 1);
        for (Operation<KeyType, ValueType> op : currentTxn.getOperations()) {
            KeyType k = op.getKey();
            ValueType v = op.getValue();
            if (op.getType() == OpType.read) {
                if (!intKeys.containsKey(k)) {
                    // check EXT
                    TidVal<ValueType> tidVal = lastCommittedTxn.getCommitFrontierTidVal().getOrDefault(k,
                            new TidVal<>(initialTxn.getTransactionId(), initialTxn.getOperations().get(0).getValue()));
                    if (!Objects.equals(tidVal.getRight(), v)) {
                        // violate EXT so far
                        EXT<KeyType, ValueType> ext = new EXT<>(tidVal.getLeft(), currentTxn, k, tidVal.getRight(), v);
                        currentTxn.getExtViolations().add(ext);
                        if (Arg.LOG_EXT_FLIP) {
                            System.out.println("EXT created when checking " +
                                    currentTxn.getTransactionId() + " on " + k + " at " + System.currentTimeMillis());
                        }
                    }
                } else if (!Objects.equals(intKeys.get(k), v)) {
                    violations.add(new INT<>(currentTxn, k, intKeys.get(k), v));
                }
            } else {
                extWriteKeys.put(k, v);
            }
            intKeys.put(k, v);
        }
        currentTxn.setExtWriteKeys(extWriteKeys);
        HashMap<KeyType, TidVal<ValueType>> commitFrontierTidVal =
                new HashMap<>(lastCommittedTxn.getCommitFrontierTidVal());
        // update commitFrontier
        for (Map.Entry<KeyType, ValueType> kv : extWriteKeys.entrySet()) {
            commitFrontierTidVal.put(kv.getKey(), new TidVal<>(currentTxn.getTransactionId(), kv.getValue()));
        }
        currentTxn.setCommitFrontierTidVal(commitFrontierTidVal);
        // re-check EXT
        lastCommittedTxn = currentTxn;
        HashSet<KeyType> writeKeys = new HashSet<>(currentTxn.getExtWriteKeys().keySet());
        int txnIdxInMemory = history.getCommitEntryIndexInMemory() + 1;
        HashSet<Transaction<KeyType, ValueType>> updateTxns = new HashSet<>();
        String path = cacheDir + System.currentTimeMillis();
        for (int i = history.getCommitEntryIndex() + 1; i < tidEntryWhetherGc.size(); i++) {
            Transaction<KeyType, ValueType> txn;
            if (!tidEntryWhetherGc.get(i).getRight()) {
                txn = txns.get(txnIdxInMemory);
                txnIdxInMemory++;
            } else {
                txn = GcUtil.readTxn(tidEntryWhetherGc.get(i).getLeft());
                assert txn != null;
                updateTxns.add(txn);
                GcUtil.tidEntryToFile.put(txn.getTransactionId(), path);
            }
            txn.recheckExt(lastCommittedTxn, currentTxn, initialTxn);
            lastCommittedTxn = txn;
            writeKeys.removeIf(k -> txn.getExtWriteKeys().containsKey(k));
            if (writeKeys.isEmpty()) {
                break;
            }
            commitFrontierTidVal = txn.getCommitFrontierTidVal();
            for (KeyType k : writeKeys) {
                commitFrontierTidVal.put(k,
                        new TidVal<>(currentTxn.getTransactionId(), currentTxn.getExtWriteKeys().get(k)));
            }
        }
        if (!updateTxns.isEmpty()) {
            try (Output output = new Output(new GZIPOutputStream(Files.newOutputStream(Paths.get(path))))) {
                GcUtil.checkKryo.writeObject(output, updateTxns);
                output.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        timer.schedule(new MarkTimeoutTask(currentTxn), Arg.TIMEOUT_DELAY);
        return violations;
    }

    @Override
    public <KeyType, ValueType> void saveToFile(History<KeyType, ValueType> history) {

    }

    @Override
    public <KeyType, ValueType> int gc(History<KeyType, ValueType> history) {
        ArrayList<Transaction<KeyType, ValueType>> txns = history.getTransactions();
        ArrayList<Pair<String, Boolean>> tidEntryWhetherGc = history.getTidEntryWhetherGc();
        int inMemoryIndex = 1;
        HashSet<Transaction<KeyType, ValueType>> toRemove = new HashSet<>(6667);
        String path = cacheDir + System.currentTimeMillis();
        for (int i = 1; i < tidEntryWhetherGc.size(); i++) {
            if (tidEntryWhetherGc.get(i).getRight()) {
                continue;
            }
            Transaction<KeyType, ValueType> txn = txns.get(inMemoryIndex);
            if (System.currentTimeMillis() - txn.getRealtimeTimestamp() > Arg.DURATION_IN_MEMORY && txn.isTimeout()) {
                toRemove.add(txn);
                GcUtil.tidEntryToFile.put(txn.getTransactionId(), path);
                tidEntryWhetherGc.set(i, Pair.of(txn.getTransactionId(), true));
            }
            inMemoryIndex++;
            if (toRemove.size() >= 16000) {
                break;
            }
        }
        if (!toRemove.isEmpty()) {
            try (Output output = new Output(new GZIPOutputStream(Files.newOutputStream(Paths.get(path))))) {
                serializeKryo.writeObject(output, toRemove);
                output.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            txns.removeAll(toRemove);
        }
        return toRemove.size();
    }
}
