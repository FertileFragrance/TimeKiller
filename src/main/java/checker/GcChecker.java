package checker;

import history.History;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;
import history.transaction.TransactionEntry;
import violation.EXT;
import violation.INT;
import violation.NOCONFLICT;
import violation.Violation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class GcChecker implements Checker {
    @Override
    public <KeyType, ValueType> ArrayList<Violation> check(History<KeyType, ValueType> history) {
        ArrayList<Violation> violations = new ArrayList<>();
        HashMap<Operation<KeyType, ValueType>, EXT<KeyType, ValueType>> incompleteExts = new HashMap<>(9);
        HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> keyWritten = history.getKeyWritten();
        HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> keyOngoing = new HashMap<>(keyWritten.size() * 4 / 3 + 1);
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
                            ArrayList<Transaction<KeyType, ValueType>> writeToKeyTxns = keyWritten.get(k);
                            Transaction<KeyType, ValueType> previousTxn = writeToKeyTxns.get(writeToKeyTxns.size() - 1);
                            if (!Objects.equals(previousTxn.getExtWriteKeys().get(k), v)) {
                                // violate EXT
                                EXT<KeyType, ValueType> extViolation = new EXT<>(previousTxn,
                                        currentTxn, k, previousTxn.getExtWriteKeys().get(k), v);
                                violations.add(extViolation);
                                for (int j = ongoingTxns.size() - 1; j >= 0; j--) {
                                    Transaction<KeyType, ValueType> writeTxn = ongoingTxns.get(j);
                                    if (Objects.equals(writeTxn.getExtWriteKeys().get(k), v)) {
                                        extViolation.setWriteLatterValueTxn(writeTxn);
                                        extViolation.setExtType(EXT.EXTType.UNCOMMITTED);
                                        break;
                                    }
                                }
                                for (int j = writeToKeyTxns.size() - 2; j >= 0; j--) {
                                    Transaction<KeyType, ValueType> writeTxn = writeToKeyTxns.get(j);
                                    if (Objects.equals(writeTxn.getExtWriteKeys().get(k), v)) {
                                        extViolation.setWriteLatterValueTxn(writeTxn);
                                        extViolation.setExtType(EXT.EXTType.BEFORE);
                                        break;
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
                            // check NOCONFLICT
                            for (int j = ongoingTxns.size() - 1; j >= 0; j--) {
                                violations.add(new NOCONFLICT<>(ongoingTxns.get(j), currentTxn, k));
                            }
                            ongoingTxns.add(currentTxn);
                        }
                        extWriteKeys.put(k, v);

                    }
                    intKeys.put(k, v);
                }
                currentTxn.setExtWriteKeys(extWriteKeys);
            } else {
                for (KeyType k : currentTxn.getExtWriteKeys().keySet()) {
                    keyOngoing.get(k).remove(currentTxn);
                    keyWritten.get(k).add(currentTxn);
                }
            }
        }
        return violations;
    }
}
