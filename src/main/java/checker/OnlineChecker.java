package checker;

import checker.online.GcTask;
import checker.online.MarkTimeoutTask;
import history.History;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;
import history.transaction.TransactionEntry;
import info.Arg;
import org.apache.commons.lang3.tuple.Pair;
import violation.EXT;
import violation.INT;
import violation.Violation;

import java.io.*;
import java.util.*;

public class OnlineChecker implements Checker {
    private final Timer timer = new Timer();

    private final String cacheDir = System.getProperty("user.dir") + "/.cache/TimeKiller/";

    @Override
    public <KeyType, ValueType> ArrayList<Violation> check(History<KeyType, ValueType> history) {
        if (history.getStartEntryIndex() <= serializedEntryIndex) {
            continueSerializing = false;
        }
        ArrayList<Violation> violations = new ArrayList<>();
        ArrayList<TransactionEntry<KeyType, ValueType>> txnEntries = history.getTransactionEntries();
        ArrayList<Pair<String, Boolean>> txnIdWhetherGc = history.getTxnIdWhetherGc();
        Transaction<KeyType, ValueType> currentTxn = txnEntries
                .get(history.getStartEntryIndexInMemory()).getTransaction();
        Transaction<KeyType, ValueType> lastCommittedTxn = null;
        int entryIndexInMemory = history.getStartEntryIndexInMemory() - 1;
        for (int i = history.getStartEntryIndex() - 1; i >= 0; i--) {
            TransactionEntry<KeyType, ValueType> lastCommittedEntry = null;
            if (!txnIdWhetherGc.get(i).getRight()) {
                lastCommittedEntry = txnEntries.get(entryIndexInMemory);
                entryIndexInMemory--;
            } else {
                try (FileInputStream fileIn = new FileInputStream(cacheDir + txnIdWhetherGc.get(i).getLeft());
                     ObjectInputStream objIn = new ObjectInputStream(fileIn)) {
                    lastCommittedEntry = (TransactionEntry<KeyType, ValueType>) objIn.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
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
                    Transaction<KeyType, ValueType> previousTxn = lastCommittedTxn.getCommitFrontier()
                            .getOrDefault(k, history.getInitialTxn());
                    if (!Objects.equals(previousTxn.getExtWriteKeys().get(k), v)) {
                        // violate EXT so far
                        currentTxn.getExtViolations().add(new EXT<>(previousTxn,
                                currentTxn, k, previousTxn.getExtWriteKeys().get(k), v));
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
        HashMap<KeyType, Transaction<KeyType, ValueType>> commitFrontier =
                new HashMap<>(lastCommittedTxn.getCommitFrontier());
        // update commitFrontier in between
        entryIndexInMemory = history.getStartEntryIndexInMemory() + 1;
        for (int i = history.getStartEntryIndex() + 1; i < history.getCommitEntryIndex(); i++) {
            TransactionEntry<KeyType, ValueType> entry = null;
            if (!txnIdWhetherGc.get(i).getRight()) {
                entry = txnEntries.get(entryIndexInMemory);
                entryIndexInMemory++;
            } else {
                try (FileInputStream fileIn = new FileInputStream(cacheDir + txnIdWhetherGc.get(i).getLeft());
                     ObjectInputStream objIn = new ObjectInputStream(fileIn)) {
                    entry = (TransactionEntry<KeyType, ValueType>) objIn.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
            assert entry != null;
            if (entry.getEntryType() == TransactionEntry.EntryType.COMMIT) {
                for (KeyType k : entry.getTransaction().getExtWriteKeys().keySet()) {
                    commitFrontier.put(k, entry.getTransaction());
                }
            }
        }
        for (KeyType k : extWriteKeys.keySet()) {
            commitFrontier.put(k, currentTxn);
        }
        currentTxn.setCommitFrontier(commitFrontier);
        // re-check EXT
        lastCommittedTxn = currentTxn;
        HashSet<KeyType> writeKeys = new HashSet<>(currentTxn.getExtWriteKeys().keySet());
        entryIndexInMemory = history.getCommitEntryIndexInMemory() + 1;
        for (int i = history.getCommitEntryIndex() + 1; i < txnIdWhetherGc.size(); i++) {
            TransactionEntry<KeyType, ValueType> entry = null;
            if (!txnIdWhetherGc.get(i).getRight()) {
                entry = txnEntries.get(entryIndexInMemory);
                entryIndexInMemory++;
            } else {
                try (FileInputStream fileIn = new FileInputStream(cacheDir + txnIdWhetherGc.get(i).getLeft());
                     ObjectInputStream objIn = new ObjectInputStream(fileIn)) {
                    entry = (TransactionEntry<KeyType, ValueType>) objIn.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
            assert entry != null;
            Transaction<KeyType, ValueType> recheckTxn = entry.getTransaction();
            if (entry.getEntryType() == TransactionEntry.EntryType.START) {
                recheckTxn.recheckExt(lastCommittedTxn, currentTxn, history.getInitialTxn());
            } else {
                lastCommittedTxn = recheckTxn;
                writeKeys.removeIf(k -> recheckTxn.getExtWriteKeys().containsKey(k));
                if (writeKeys.isEmpty()) {
                    break;
                }
                commitFrontier = recheckTxn.getCommitFrontier();
                for (KeyType k : writeKeys) {
                    commitFrontier.put(k, currentTxn);
                }
                if (txnIdWhetherGc.get(i).getRight()) {
                    try (FileOutputStream fileOut = new FileOutputStream(cacheDir + recheckTxn.getTransactionId());
                         ObjectOutputStream objOut = new ObjectOutputStream(fileOut)) {
                        objOut.writeObject(entry);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        timer.schedule(new MarkTimeoutTask(currentTxn), Arg.TIMEOUT_DELAY);
        return violations;
    }

    @Override
    public <KeyType, ValueType> void saveToFile(History<KeyType, ValueType> history) {

    }

    public <KeyType, ValueType> Pair<ArrayList<Integer>, ArrayList<Integer>> preGc(History<KeyType, ValueType> history) {
        GcTask.gcLock.lock();
        ArrayList<TransactionEntry<KeyType, ValueType>> txnEntries = history.getTransactionEntries();
        HashSet<Transaction<KeyType, ValueType>> remain = new HashSet<>(txnEntries
                .get(txnEntries.size() - 1).getTransaction().getCommitFrontier().values());
        ArrayList<Pair<String, Boolean>> txnIdWhetherGc = history.getTxnIdWhetherGc();
        ArrayList<Integer> allIndexes = new ArrayList<>();
        ArrayList<Integer> inMemoryIndexes = new ArrayList<>();
        int inMemoryIndex = 2;
        for (int i = 2; i < txnIdWhetherGc.size(); i++) {
            if (txnIdWhetherGc.get(i).getRight()) {
                continue;
            }
            TransactionEntry<KeyType, ValueType> entry = txnEntries.get(inMemoryIndex);
            if (!remain.contains(entry.getTransaction()) && entry.getTransaction().isTimeout()) {
                allIndexes.add(i);
                inMemoryIndexes.add(inMemoryIndex);
            }
            inMemoryIndex++;
            if (allIndexes.size() == Arg.MAX_NUM_EACH_GC) {
                break;
            }
        }
        GcTask.gcLock.unlock();
        return Pair.of(allIndexes, inMemoryIndexes);
    }

    private static int serializedEntryIndex = 1;
    private static boolean continueSerializing = true;

    public <KeyType, ValueType> int serialize(Pair<ArrayList<Integer>, ArrayList<Integer>> allAndInMemoryIndexes,
                                              History<KeyType, ValueType> history) {
        continueSerializing = true;
        ArrayList<Integer> allIndexes = allAndInMemoryIndexes.getLeft();
        ArrayList<Integer> inMemoryIndexes = allAndInMemoryIndexes.getRight();
        for (int i = 0; i < allIndexes.size(); i++) {
            if (!continueSerializing) {
                return i;
            }
            TransactionEntry<KeyType, ValueType> entry = history.getTransactionEntries().get(inMemoryIndexes.get(i));
            String txnId = entry.getTransaction().getTransactionId();
            String t;
            if (entry.getEntryType() == TransactionEntry.EntryType.START) {
                t = "-s";
            } else {
                t = "-c";
            }
            try (FileOutputStream fileOut = new FileOutputStream(cacheDir + txnId + t);
                 ObjectOutputStream objOut = new ObjectOutputStream(fileOut)) {
                objOut.writeObject(entry);
            } catch (IOException e) {
                e.printStackTrace();
            }
            serializedEntryIndex = allIndexes.get(i);
        }
        return allIndexes.size();
    }

    public <KeyType, ValueType> void doGc(Pair<ArrayList<Integer>, ArrayList<Integer>> allAndInMemoryIndexes,
                                          History<KeyType, ValueType> history, int gcSize) {
        GcTask.gcLock.lock();
        ArrayList<Integer> allIndexes = allAndInMemoryIndexes.getLeft();
        ArrayList<Integer> inMemoryIndexes = allAndInMemoryIndexes.getRight();
        ArrayList<TransactionEntry<KeyType, ValueType>> txnEntries = history.getTransactionEntries();
        HashSet<TransactionEntry<KeyType, ValueType>> toRemove = new HashSet<>(gcSize * 4 / 3 + 1);
        for (int i = 0; i < gcSize; i++) {
            TransactionEntry<KeyType, ValueType> entry = txnEntries.get(inMemoryIndexes.get(i));
            toRemove.add(entry);
            String t;
            if (entry.getEntryType() == TransactionEntry.EntryType.START) {
                t = "-s";
            } else {
                t = "-c";
            }
            history.getTxnIdWhetherGc().set(allIndexes.get(i), Pair.of(entry.getTransaction().getTransactionId() + t, true));
        }
        txnEntries.removeAll(toRemove);
        if (!toRemove.isEmpty()) {
            System.out.println(toRemove.size() + " entries are removed. " + txnEntries.size() + " entries are left");
        }
        GcTask.gcLock.unlock();
    }
}
