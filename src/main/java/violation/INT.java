package violation;

import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;

public class INT<KeyType, ValueType> extends Violation {
    private final Transaction<KeyType, ValueType> txn;
    private final KeyType key;
    private final ValueType formerValue;
    private final ValueType latterValue;

    public INT(Transaction<KeyType, ValueType> txn, KeyType key, ValueType formerValue, ValueType latterValue) {
        this.type = ViolationType.INT;
        this.txn = txn;
        this.key = key;
        this.formerValue = formerValue;
        this.latterValue = latterValue;
    }

    @Override
    public void fix() {
        ValueType lastValue = null;
        boolean first = true;
        for (Operation<KeyType, ValueType> operation : txn.getOperations()) {
            if (!key.equals(operation.getKey())) {
                continue;
            }
            if (operation.getType() == OpType.write || first) {
                lastValue = operation.getValue();
                first = false;
            } else {
                operation.setValue(lastValue);
            }
        }
    }

    @Override
    public String toString() {
        String s1 = "Violation of INT is found ";
        return s1 + "{" +
                "txn=" + txn +
                ", key=" + key +
                ", formerValue=" + formerValue +
                ", latterValue=" + latterValue +
                '}';
    }

    public Transaction<KeyType, ValueType> getTxn() {
        return txn;
    }

    public KeyType getKey() {
        return key;
    }

    public ValueType getFormerValue() {
        return formerValue;
    }

    public ValueType getLatterValue() {
        return latterValue;
    }
}
