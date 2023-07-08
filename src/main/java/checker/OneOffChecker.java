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
                if (op.getType() == OpType.read) {
                    if (!intKeys.containsKey(op.getKey())) {
                        // check EXT
                        ArrayList<Transaction<KeyType, ValueType>> writeToKeyTxns = keyWritten.get(op.getKey());
                        for (int j = writeToKeyTxns.size() - 1; j >= 0; j--) {
                            Transaction<KeyType, ValueType> previousTxn = writeToKeyTxns.get(j);
                            if (previousTxn.getCommitTimestamp().compareTo(currentTxn.getStartTimestamp()) <= 0) {
                                if (previousTxn.getExtWriteKeys().get(op.getKey()).equals(op.getValue())) {
                                    // violate EXT
                                    violations.add(new EXT<>(previousTxn, currentTxn, op.getKey(),
                                            previousTxn.getExtWriteKeys().get(op.getKey()), op.getValue()));
                                }
                                break;
                            }
                        }
                    } else if (!intKeys.get(op.getKey()).equals(op.getValue())) {
                        // violate INT
                        violations.add(new INT<>(currentTxn, op.getKey(), intKeys.get(op.getKey()), op.getValue()));
                    }
                } else {
                    if (!extWriteKeys.containsKey(op.getKey())) {
                        // check NOCONFLICT
                        ArrayList<Transaction<KeyType, ValueType>> writeToKeyTxns = keyWritten.get(op.getKey());
                        for (int j = writeToKeyTxns.size() - 1; j >= 0; j--) {
                            Transaction<KeyType, ValueType> previousTxn = writeToKeyTxns.get(j);
                            if (previousTxn.getCommitTimestamp().compareTo(currentTxn.getStartTimestamp()) > 0) {
                                // violate NOCONFLICT
                                violations.add(new NOCONFLICT<>(previousTxn, currentTxn, op.getKey()));
                            } else {
                                break;
                            }
                        }
                    }
                    extWriteKeys.put(op.getKey(), op.getValue());
                    keyWritten.get(op.getKey()).add(currentTxn);
                }
                intKeys.put(op.getKey(), op.getValue());
            }
            currentTxn.setExtWriteKeys(extWriteKeys);
        }
        return violations;
    }
}
