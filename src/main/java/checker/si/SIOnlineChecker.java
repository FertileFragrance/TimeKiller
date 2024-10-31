package checker.si;

import checker.OnlineChecker;
import checker.online.GcTask;
import checker.online.MarkTimeoutTask;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import history.History;
import history.transaction.*;
import info.Arg;
import checker.online.GcUtil;
import org.apache.commons.lang3.tuple.Pair;
import violation.EXT;
import violation.INT;
import violation.Violation;
import violation.ViolationType;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public class SIOnlineChecker implements OnlineChecker {
    private final Timer timer = new Timer();

    private final String cacheDir = System.getProperty("user.dir") + "/.cache/TimeKiller/";

    private final Kryo serializeKryo;

    public SIOnlineChecker() {
        serializeKryo = new Kryo();
        serializeKryo.register(HashSet.class);
        serializeKryo.register(TransactionEntry.class);
        serializeKryo.register(Transaction.class);
        serializeKryo.register(TransactionEntry.EntryType.class);
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
        ArrayList<TransactionEntry<KeyType, ValueType>> txnEntries = history.getTransactionEntries();
        ArrayList<Pair<String, Boolean>> tidEntryWhetherGc = history.getTidEntryWhetherGc();
        Transaction<KeyType, ValueType> currentTxn = txnEntries
                .get(history.getStartEntryIndexInMemory()).getTransaction();
        Transaction<KeyType, ValueType> lastCommittedTxn = null;
        int entryIndexInMemory = history.getStartEntryIndexInMemory() - 1;
        for (int i = history.getStartEntryIndex() - 1; i >= 0; i--) {
            TransactionEntry<KeyType, ValueType> lastCommittedEntry;
            if (!tidEntryWhetherGc.get(i).getRight()) {
                lastCommittedEntry = txnEntries.get(entryIndexInMemory);
                entryIndexInMemory--;
            } else {
                lastCommittedEntry = GcUtil.readTxnEntry(tidEntryWhetherGc.get(i).getLeft());
            }
            assert lastCommittedEntry != null;
            if (lastCommittedEntry.getEntryType() == TransactionEntry.EntryType.COMMIT) {
                lastCommittedTxn = lastCommittedEntry.getTransaction();
                break;
            }
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
        // update commitFrontier in between
        entryIndexInMemory = history.getStartEntryIndexInMemory() + 1;
        for (int i = history.getStartEntryIndex() + 1; i < history.getCommitEntryIndex(); i++) {
            TransactionEntry<KeyType, ValueType> entry;
            if (!tidEntryWhetherGc.get(i).getRight()) {
                entry = txnEntries.get(entryIndexInMemory);
                entryIndexInMemory++;
            } else {
                entry = GcUtil.readTxnEntry(tidEntryWhetherGc.get(i).getLeft());
            }
            assert entry != null;
            if (entry.getEntryType() == TransactionEntry.EntryType.COMMIT) {
                for (Map.Entry<KeyType, ValueType> kv : entry.getTransaction().getExtWriteKeys().entrySet()) {
                    commitFrontierTidVal.put(kv.getKey(),
                            new TidVal<>(entry.getTransaction().getTransactionId(), kv.getValue()));
                }
            }
        }
        for (Map.Entry<KeyType, ValueType> kv : extWriteKeys.entrySet()) {
            commitFrontierTidVal.put(kv.getKey(), new TidVal<>(currentTxn.getTransactionId(), kv.getValue()));
        }
        currentTxn.setCommitFrontierTidVal(commitFrontierTidVal);
        // re-check EXT
        lastCommittedTxn = currentTxn;
        HashSet<KeyType> writeKeys = new HashSet<>(currentTxn.getExtWriteKeys().keySet());
        entryIndexInMemory = history.getCommitEntryIndexInMemory() + 1;
        HashSet<TransactionEntry<KeyType, ValueType>> update = new HashSet<>();
        String path = cacheDir + System.currentTimeMillis();
        for (int i = history.getCommitEntryIndex() + 1; i < tidEntryWhetherGc.size(); i++) {
            TransactionEntry<KeyType, ValueType> entry;
            if (!tidEntryWhetherGc.get(i).getRight()) {
                entry = txnEntries.get(entryIndexInMemory);
                entryIndexInMemory++;
            } else {
                entry = GcUtil.readTxnEntry(tidEntryWhetherGc.get(i).getLeft());
                update.add(entry);
                String tidEntry = entry.getTransaction().getTransactionId();
                if (entry.getEntryType() == TransactionEntry.EntryType.START) {
                    tidEntry += "-s";
                } else {
                    tidEntry += "-c";
                }
                GcUtil.tidEntryToFile.put(tidEntry, path);
            }
            assert entry != null;
            Transaction<KeyType, ValueType> recheckTxn = entry.getTransaction();
            if (entry.getEntryType() == TransactionEntry.EntryType.START) {
                recheckTxn.recheckExt(lastCommittedTxn, currentTxn, initialTxn);
            } else {
                lastCommittedTxn = recheckTxn;
                writeKeys.removeIf(k -> recheckTxn.getExtWriteKeys().containsKey(k));
                if (writeKeys.isEmpty()) {
                    break;
                }
                commitFrontierTidVal = recheckTxn.getCommitFrontierTidVal();
                for (KeyType k : writeKeys) {
                    commitFrontierTidVal.put(k,
                            new TidVal<>(currentTxn.getTransactionId(), currentTxn.getExtWriteKeys().get(k)));
                }
            }
        }
        if (!update.isEmpty()) {
            try (Output output = new Output(new GZIPOutputStream(Files.newOutputStream(Paths.get(path))))) {
                GcUtil.checkKryo.writeObject(output, update);
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
        ArrayList<TransactionEntry<KeyType, ValueType>> txnEntries = history.getTransactionEntries();
        ArrayList<Pair<String, Boolean>> tidEntryWhetherGc = history.getTidEntryWhetherGc();
        int inMemoryIndex = 2;
        HashSet<TransactionEntry<KeyType, ValueType>> toRemove = new HashSet<>(6667);
        String path = cacheDir + System.currentTimeMillis();
        for (int i = 2; i < tidEntryWhetherGc.size(); i++) {
            if (tidEntryWhetherGc.get(i).getRight()) {
                continue;
            }
            TransactionEntry<KeyType, ValueType> entry = txnEntries.get(inMemoryIndex);
            if (System.currentTimeMillis() - entry.getTransaction().getRealtimeTimestamp() > Arg.DURATION_IN_MEMORY
                    && entry.getTransaction().isTimeout()) {
                String tidEntry = entry.getTransaction().getTransactionId();
                if (entry.getEntryType() == TransactionEntry.EntryType.START) {
                    tidEntry += "-s";
                } else {
                    tidEntry += "-c";
                }
                toRemove.add(entry);
                GcUtil.tidEntryToFile.put(tidEntry, path);
                tidEntryWhetherGc.set(i, Pair.of(tidEntry, true));
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
            txnEntries.removeAll(toRemove);
//            System.out.println(toRemove.size() / 2 + " txns are removed. " + txnEntries.size() / 2 + " txns are left");
        }
        return toRemove.size();
    }

    private final HashSet<TransactionEntry<?, ?>> remove = new HashSet<>(6667);
    private String savePath = null;
    private final HashMap<Integer, String> idx2TidEntry = new HashMap<>(16000);

    @Override
    public <KeyType, ValueType> void preGc(History<KeyType, ValueType> history) {
        ArrayList<TransactionEntry<KeyType, ValueType>> txnEntries = history.getTransactionEntries();
        ArrayList<Pair<String, Boolean>> tidEntryWhetherGc = history.getTidEntryWhetherGc();
        int inMemoryIndex = 2;
        savePath = cacheDir + System.currentTimeMillis();
        for (int i = 2; i < tidEntryWhetherGc.size(); i++) {
            if (tidEntryWhetherGc.get(i).getRight()) {
                continue;
            }
            TransactionEntry<KeyType, ValueType> entry = txnEntries.get(inMemoryIndex);
            if (System.currentTimeMillis() - entry.getTransaction().getRealtimeTimestamp() > Arg.DURATION_IN_MEMORY
                    && entry.getTransaction().isTimeout()) {
                String tidEntry = entry.getTransaction().getTransactionId();
                if (entry.getEntryType() == TransactionEntry.EntryType.START) {
                    tidEntry += "-s";
                } else {
                    tidEntry += "-c";
                }
                remove.add(entry);
                idx2TidEntry.put(i, tidEntry);
                GcTask.maxTimestampInRemove = entry.getTimestamp();
            }
            inMemoryIndex++;
            if (remove.size() >= 16000) {
                break;
            }
        }
    }

    @Override
    public void performGc() {
        if (!remove.isEmpty()) {
            try (Output output = new Output(new GZIPOutputStream(Files.newOutputStream(Paths.get(savePath))))) {
                serializeKryo.writeObject(output, remove);
                output.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public <KeyType, ValueType> int postGc(History<KeyType, ValueType> history) {
        int removedSize = remove.size();
        ArrayList<Pair<String, Boolean>> tidEntryWhetherGc = history.getTidEntryWhetherGc();
        for (Map.Entry<Integer, String> entry : idx2TidEntry.entrySet()) {
            int i = entry.getKey();
            String tidEntry = entry.getValue();
            GcUtil.tidEntryToFile.put(tidEntry, savePath);
            tidEntryWhetherGc.set(i, Pair.of(tidEntry, true));
        }
        history.getTransactionEntries().removeAll(remove);
        remove.clear();
        savePath = null;
        idx2TidEntry.clear();
        GcTask.maxTimestampInRemove = history.getInitialTxn().getStartTimestamp();
        return removedSize;
    }
}
