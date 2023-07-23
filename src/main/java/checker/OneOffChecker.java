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
        ArrayList<Transaction<KeyType, ValueType>> txns = history.getTransactions();
        HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> keyWritten = history.getKeyWritten();
        for (Transaction<KeyType, ValueType> currentTxn : txns) {
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
                                    violations.add(new EXT<>(previousTxn, currentTxn, k,
                                            previousTxn.getExtWriteKeys().get(k), v));
                                }
                                break;
                            }
                        }
                    } else if (!Objects.equals(intKeys.get(k), v)) {
                        // violate INT
                        violations.add(new INT<>(currentTxn, k, intKeys.get(k), v));
                    }
                } else {
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
