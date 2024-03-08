package history.transaction;

import com.alibaba.fastjson.annotation.JSONField;
import info.Arg;
import violation.EXT;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class Transaction<KeyType, ValueType> implements Serializable {
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

    @JSONField(serialize = false)
    private Long realtimeTimestamp;

    @JSONField(serialize = false)
    private HashMap<KeyType, Transaction<KeyType, ValueType>> commitFrontier;

    @JSONField(serialize = false)
    private boolean timeout;

    @JSONField(serialize = false)
    private List<EXT<KeyType, ValueType>> extViolations;

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

    public Long getRealtimeTimestamp() {
        return realtimeTimestamp;
    }

    public void setRealtimeTimestamp(Long realtimeTimestamp) {
        this.realtimeTimestamp = realtimeTimestamp;
    }

    public HashMap<KeyType, Transaction<KeyType, ValueType>> getCommitFrontier() {
        return commitFrontier;
    }

    public void setCommitFrontier(HashMap<KeyType, Transaction<KeyType, ValueType>> commitFrontier) {
        this.commitFrontier = commitFrontier;
    }

    public boolean isTimeout() {
        return timeout;
    }

    public List<EXT<KeyType, ValueType>> getExtViolations() {
        return extViolations;
    }

    public Transaction(String transactionId, String sessionId, ArrayList<Operation<KeyType, ValueType>> operations,
                       HybridLogicalClock startTimestamp, HybridLogicalClock commitTimestamp) {
        this.transactionId = transactionId;
        this.sessionId = sessionId;
        this.operations = operations;
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = commitTimestamp;
        if ("online".equals(Arg.MODE)) {
            timeout = false;
            extViolations = new ArrayList<>();
        }
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
                ", sid=" + sessionId +
                ", ops=" + operations +
                ", startTs=" + startTimestamp +
                ", commitTs=" + commitTimestamp +
                '}';
    }

    public synchronized void markTimeout() {
        timeout = true;
        extViolations.forEach(System.out::println);
    }

    public synchronized void recheckExt(Transaction<KeyType, ValueType> lastCommittedTxn,
                                        Transaction<KeyType, ValueType> currentTxn,
                                        Transaction<KeyType, ValueType> initialTxn) {
        if (timeout) {
            return;
        }
        HashMap<KeyType, ValueType> intKeys = new HashMap<>(operations.size() * 4 / 3 + 1);
        for (Operation<KeyType, ValueType> op : operations) {
            KeyType k = op.getKey();
            ValueType v = op.getValue();
            if (op.getType() == OpType.read && !intKeys.containsKey(k)
                    && currentTxn.getExtWriteKeys().containsKey(k)) {
                Transaction<KeyType, ValueType> previousTxn = lastCommittedTxn
                        .getCommitFrontier().getOrDefault(k, initialTxn);
                if (!Objects.equals(previousTxn.getExtWriteKeys().get(k), v)) {
                    // add a new EXT violation or update an existing EXT violation
                    boolean addANewOne = true;
                    for (EXT<KeyType, ValueType> extViolation : extViolations) {
                        if (extViolation.getKey().equals(k)) {
                            if (!extViolation.getFormerTxnId().equals(previousTxn.getTransactionId())) {
                                extViolation.setFormerTxnId(previousTxn.getTransactionId());
                                extViolation.setFormerValue(previousTxn.getExtWriteKeys().get(k));
                            }
                            addANewOne = false;
                            break;
                        }
                    }
                    if (addANewOne) {
                        extViolations.add(new EXT<>(previousTxn.getTransactionId(),
                                this, k, previousTxn.getExtWriteKeys().get(k), v));
                    }
                } else {
                    // remove a potential EXT violation
                    extViolations.removeIf(extViolation -> extViolation.getKey().equals(k));
                }
            }
            intKeys.put(k, v);
        }
    }
}
