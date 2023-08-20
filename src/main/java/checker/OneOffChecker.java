package checker;

import history.History;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;
import violation.EXT;
import violation.INT;
import violation.NOCONFLICT;
import violation.Violation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class OneOffChecker {
    public static <KeyType, ValueType> ArrayList<Violation> check(History<KeyType, ValueType> history) {
        ArrayList<Violation> violations = new ArrayList<>();
        HashMap<Operation<KeyType, ValueType>, EXT<KeyType, ValueType>> incompleteExts = new HashMap<>(9);
        ArrayList<Transaction<KeyType, ValueType>> txns = history.getTransactions();
        HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> keyWritten = history.getKeyWritten();
        for (int i = 1; i < txns.size(); i++) {
            Transaction<KeyType, ValueType> currentTxn = txns.get(i);
            int opSize = currentTxn.getOperations().size();
            HashMap<KeyType, ValueType> intKeys = new HashMap<>(opSize * 4 / 3 + 1);
            HashMap<KeyType, ValueType> extWriteKeys = new HashMap<>(opSize * 4 / 3 + 1);
            for (Operation<KeyType, ValueType> op : currentTxn.getOperations()) {
                KeyType k = op.getKey();
                ValueType v = op.getValue();
                if (op.getType() == OpType.read) {
                    if (!intKeys.containsKey(k)) {
                        // check EXT
                        ArrayList<Transaction<KeyType, ValueType>> writeToKeyTxns = keyWritten.get(k);
                        for (int j = writeToKeyTxns.size() - 1; j >= 0; j--) {
                            Transaction<KeyType, ValueType> previousTxn = writeToKeyTxns.get(j);
                            if (previousTxn.getCommitTimestamp().compareTo(currentTxn.getStartTimestamp()) <= 0) {
                                if (!Objects.equals(previousTxn.getExtWriteKeys().get(k), v)) {
                                    // violate EXT
                                    EXT<KeyType, ValueType> extViolation = new EXT<>(previousTxn,
                                            currentTxn, k, previousTxn.getExtWriteKeys().get(k), v);
                                    violations.add(extViolation);
                                    for (int jj = writeToKeyTxns.size() - 1; jj >= 0; jj--) {
                                        if (jj == j) {
                                            continue;
                                        }
                                        Transaction<KeyType, ValueType> writeTxn = writeToKeyTxns.get(jj);
                                        if (Objects.equals(writeTxn.getExtWriteKeys().get(k), v)) {
                                            extViolation.setWriteLatterValueTxn(writeTxn);
                                            if (jj > j) {
                                                extViolation.setExtType(EXT.EXTType.UNCOMMITTED);
                                            } else {
                                                extViolation.setExtType(EXT.EXTType.BEFORE);
                                            }
                                            break;
                                        }
                                        if (jj == 0) {
                                            incompleteExts.put(new Operation<>(OpType.write, k, v), extViolation);
                                        }
                                    }
                                }
                                break;
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
                        ArrayList<Transaction<KeyType, ValueType>> writeToKeyTxns = keyWritten.get(k);
                        for (int j = writeToKeyTxns.size() - 1; j >= 0; j--) {
                            Transaction<KeyType, ValueType> previousTxn = writeToKeyTxns.get(j);
                            if (previousTxn.getCommitTimestamp().compareTo(currentTxn.getStartTimestamp()) > 0) {
                                // violate NOCONFLICT
                                violations.add(new NOCONFLICT<>(previousTxn, currentTxn, k));
                            } else {
                                break;
                            }
                        }
                    }
                    extWriteKeys.put(k, v);
                    keyWritten.get(k).add(currentTxn);
                }
                intKeys.put(k, v);
            }
            currentTxn.setExtWriteKeys(extWriteKeys);
        }
        return violations;
    }
}
