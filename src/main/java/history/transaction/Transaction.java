package history.transaction;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class Transaction<KeyType, ValueType> {
    @JSONField(name = "tid")
    private final String transactionId;
    @JSONField(name = "sid")
    private final String sessionId;
    @JSONField(name = "ops")
    private final ArrayList<Operation<KeyType, ValueType>> operations;
    @JSONField(name = "sts")
    private HybridLogicalClock startTimestamp;
    @JSONField(name = "cts")
    private final HybridLogicalClock commitTimestamp;

    @JSONField(serialize = false)
    private HashMap<KeyType, ValueType> extWriteKeys;

    public String getTransactionId() {
        return transactionId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public ArrayList<Operation<KeyType, ValueType>> getOperations() {
        return operations;
    }

    public HybridLogicalClock getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(HybridLogicalClock startTimestamp) {
        this.startTimestamp = startTimestamp;
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

    public Transaction(String transactionId, String sessionId, ArrayList<Operation<KeyType, ValueType>> operations,
                       HybridLogicalClock startTimestamp, HybridLogicalClock commitTimestamp) {
        this.transactionId = transactionId;
        this.sessionId = sessionId;
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
        return "{" +
                "id=" + transactionId +
                ", ops=" + operations +
                ", startTs=" + startTimestamp +
                ", commitTS=" + commitTimestamp +
                '}';
    }
}
