package history.transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class Transaction<KeyType, ValueType> {
    private final String transactionId;
    private final ArrayList<Operation<KeyType, ValueType>> operations;
    private final HybridLogicalClock startTimestamp;
    private final HybridLogicalClock commitTimestamp;

    private HashMap<KeyType, ValueType> extWriteKeys;

    public String getTransactionId() {
        return transactionId;
    }

    public ArrayList<Operation<KeyType, ValueType>> getOperations() {
        return operations;
    }

    public HybridLogicalClock getStartTimestamp() {
        return startTimestamp;
    }

    public HybridLogicalClock getCommitTimestamp() {
        return commitTimestamp;
    }

    public HashMap<KeyType, ValueType> getExtWriteKeys() {
        return extWriteKeys;
    }

    public void setExtWriteKeys(HashMap<KeyType, ValueType> extWriteKeys) {
        this.extWriteKeys = extWriteKeys;
    }

    public Transaction(String transactionId, ArrayList<Operation<KeyType, ValueType>> operations,
                       HybridLogicalClock startTimestamp, HybridLogicalClock commitTimestamp) {
        this.transactionId = transactionId;
        this.operations = operations;
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = commitTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Transaction<?, ?>) {
            Transaction<?, ?> txn = (Transaction<?, ?>) obj;
            return this == txn || this.transactionId.equals(txn.transactionId);
        }
        return false;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId='" + transactionId + '\'' +
                ", operations=" + operations +
                ", startTimestamp=" + startTimestamp +
                ", commitTimestamp=" + commitTimestamp +
                '}';
    }
}
