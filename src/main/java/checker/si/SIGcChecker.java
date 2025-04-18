package checker.si;

import checker.Checker;
import info.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import history.History;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;
import history.transaction.TransactionEntry;
import org.apache.commons.lang3.tuple.Pair;
import violation.EXT;
import violation.INT;
import violation.NOCONFLICT;
import violation.Violation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class SIGcChecker implements Checker {
    @Override
    public <KeyType, ValueType> ArrayList<Violation> check(History<KeyType, ValueType> history) {
        ArrayList<Violation> violations = new ArrayList<>();
        HashMap<Operation<KeyType, ValueType>, EXT<KeyType, ValueType>> incompleteExts = new HashMap<>(9);
        HashMap<KeyType, Pair<String, ValueType>> frontierTidVal = history.getFrontierTidVal();
        HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> keyOngoing = new HashMap<>(history.getKeyNumber() * 4 / 3 + 1);
        int checkedEntryCount = 0;
        for (int i = 2; i < history.getTransactionEntries().size(); i++) {
            TransactionEntry<KeyType, ValueType> currentEntry = history.getTransactionEntries().get(i);
            Transaction<KeyType, ValueType> currentTxn = currentEntry.getTransaction();
            if (currentEntry.getEntryType() == TransactionEntry.EntryType.START) {
                int opSize = currentTxn.getOperations().size();
                HashMap<KeyType, ValueType> intKeys = new HashMap<>(opSize * 4 / 3 + 1);
                HashMap<KeyType, ValueType> extWriteKeys = new HashMap<>(opSize * 4 / 3 + 1);
                for (Operation<KeyType, ValueType> op : currentTxn.getOperations()) {
                    KeyType k = op.getKey();
                    ValueType v = op.getValue();
                    ArrayList<Transaction<KeyType, ValueType>> ongoingTxns = keyOngoing.computeIfAbsent(k, k1 -> new ArrayList<>(2));
                    if (op.getType() == OpType.read) {
                        if (!intKeys.containsKey(k)) {
                            // check EXT
                            if (!Objects.equals(frontierTidVal.get(k).getRight(), v)) {
                                // violate EXT
                                EXT<KeyType, ValueType> extViolation = new EXT<>(frontierTidVal.get(k).getLeft(),
                                        currentTxn, k, frontierTidVal.get(k).getRight(), v);
                                violations.add(extViolation);
                                for (int j = ongoingTxns.size() - 1; j >= 0; j--) {
                                    Transaction<KeyType, ValueType> writeTxn = ongoingTxns.get(j);
                                    if (Objects.equals(writeTxn.getExtWriteKeys().get(k), v)) {
                                        extViolation.setWriteLatterValueTxn(writeTxn);
                                        extViolation.setExtType(EXT.EXTType.UNCOMMITTED);
                                        break;
                                    }
                                }
                                if (Objects.equals(history.getInitialTxn().getExtWriteKeys().get(k), v)) {
                                    extViolation.setWriteLatterValueTxn(history.getInitialTxn());
                                    extViolation.setExtType(EXT.EXTType.BEFORE);
                                }
                                if (extViolation.getExtType() == EXT.EXTType.NEVER) {
                                    for (int j = i - 1; j >= 2; j--) {
                                        Transaction<KeyType, ValueType> writeTxn = history
                                                .getTransactionEntries().get(j).getTransaction();
                                        if (Objects.equals(writeTxn.getExtWriteKeys().get(k), v)) {
                                            extViolation.setWriteLatterValueTxn(writeTxn);
                                            extViolation.setExtType(EXT.EXTType.BEFORE);
                                            break;
                                        }
                                    }
                                }
                                if (extViolation.getExtType() == EXT.EXTType.NEVER) {
                                    incompleteExts.put(new Operation<>(OpType.write, k, v), extViolation);
                                }
                            }
                        } else if (!Objects.equals(intKeys.get(k), v)) {
                            // violate INT
                            violations.add(new INT<>(currentTxn, k, intKeys.get(k), v));
                        }
                    } else {
                        EXT<KeyType, ValueType> extViolation = incompleteExts.get(op);
                        if (extViolation != null) {
                            extViolation.setWriteLatterValueTxn(currentTxn);
                            extViolation.setExtType(EXT.EXTType.AFTER);
                            incompleteExts.remove(op, extViolation);
                        }
                        if (!extWriteKeys.containsKey(k)) {
                            ongoingTxns.add(currentTxn);
                        }
                        extWriteKeys.put(k, v);
                    }
                    intKeys.put(k, v);
                }
                currentTxn.setExtWriteKeys(extWriteKeys);
            } else {
                for (Map.Entry<KeyType, ValueType> kv : currentTxn.getExtWriteKeys().entrySet()) {
                    KeyType k = kv.getKey();
                    ArrayList<Transaction<KeyType, ValueType>> ongoingTxns = keyOngoing.get(k);
                    ongoingTxns.remove(currentTxn);
                    // check NOCONFLICT
                    for (int j = ongoingTxns.size() - 1; j >= 0; j--) {
                        violations.add(new NOCONFLICT<>(ongoingTxns.get(j), currentTxn, k));
                    }
                    frontierTidVal.put(k, Pair.of(currentTxn.getTransactionId(), kv.getValue()));
                }
            }
            checkedEntryCount++;
            if (checkedEntryCount == Arg.NUM_PER_GC * 2) {
                long gcStart = System.currentTimeMillis();
                i = gc(history, i, keyOngoing);
                System.gc();
                Stats.addGcTime(gcStart, System.currentTimeMillis());
                checkedEntryCount = 0;
            }
        }
        return violations;
    }

    public <KeyType, ValueType> int gc(History<KeyType, ValueType> history, int currentIndex,
                                       HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> keyOngoing) {
        HashSet<Transaction<KeyType, ValueType>> remain = new HashSet<>(keyOngoing.size() * 4 / 3 + 1);
        keyOngoing.forEach((k, v) -> remain.addAll(v));
        HashSet<TransactionEntry<KeyType, ValueType>> toRemove = new HashSet<>(currentIndex * 4 / 3 + 1);
        ArrayList<TransactionEntry<KeyType, ValueType>> transactionEntries = history.getTransactionEntries();
        for (int i = 2; i < currentIndex; i++) {
            TransactionEntry<KeyType, ValueType> entry = transactionEntries.get(i);
            if (!remain.contains(entry.getTransaction())) {
                toRemove.add(entry);
            }
        }
        TransactionEntry<KeyType, ValueType> currentEntry = transactionEntries.get(currentIndex);
        transactionEntries.removeAll(toRemove);
        return transactionEntries.indexOf(currentEntry);
    }

    @Override
    public <KeyType, ValueType> void saveToFile(History<KeyType, ValueType> history) {
        ArrayList<TransactionEntry<KeyType, ValueType>> entries = history.getTransactionEntries();
        ArrayList<Transaction<KeyType, ValueType>> txns = new ArrayList<>(entries.size() / 2 - 1);
        for (int i = 2; i < entries.size(); i++) {
            TransactionEntry<KeyType, ValueType> entry = entries.get(i);
            if (entry.getEntryType() == TransactionEntry.EntryType.COMMIT) {
                txns.add(entry.getTransaction());
            }
        }
        String content = JSON.toJSONString(txns, SerializerFeature.DisableCircularReferenceDetect);
        File file = new File(Arg.FILEPATH);
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(
                    new File(Arg.FILEPATH).getParent() + File.separator + "FIXED-" + file.getName()));
            out.write(content);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
