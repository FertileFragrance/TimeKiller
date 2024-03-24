package history.transaction;

import com.alibaba.fastjson.annotation.JSONField;
import info.Arg;
import violation.EXT;

import java.io.Serializable;
import java.util.*;

public class Transaction<KeyType, ValueType> implements Serializable {
    @JSONField(name = "tid")
    private String transactionId;
    @JSONField(name = "sid")
    private String sessionId;
    @JSONField(name = "ops")
    private ArrayList<Operation<KeyType, ValueType>> operations;
    @JSONField(name = "sts")
    private HybridLogicalClock startTimestamp;
    @JSONField(name = "cts")
    private HybridLogicalClock commitTimestamp;

    @JSONField(serialize = false)
    private HashMap<KeyType, ValueType> extWriteKeys;

    @JSONField(serialize = false)
    private Long realtimeTimestamp;
    
    @JSONField(serialize = false)
    private HashMap<KeyType, TidVal<ValueType>> commitFrontierTidVal;

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

    public HashMap<KeyType, TidVal<ValueType>> getCommitFrontierTidVal() {
        return commitFrontierTidVal;
    }

    public void setCommitFrontierTidVal(HashMap<KeyType, TidVal<ValueType>> commitFrontierTidVal) {
        this.commitFrontierTidVal = commitFrontierTidVal;
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

    public Transaction() {}

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
                TidVal<ValueType> tidVal = lastCommittedTxn.getCommitFrontierTidVal().getOrDefault(k,
                        new TidVal<>(initialTxn.getTransactionId(), initialTxn.getOperations().get(0).getValue()));
                if (!Objects.equals(tidVal.getRight(), v)) {
                    // add a new EXT violation or update an existing EXT violation
                    boolean addANewOne = true;
                    for (EXT<KeyType, ValueType> extViolation : extViolations) {
                        if (extViolation.getKey().equals(k)) {
                            if (!extViolation.getFormerTxnId().equals(tidVal.getLeft())) {
                                extViolation.setFormerTxnId(tidVal.getLeft());
                                extViolation.setFormerValue(tidVal.getRight());
                                if (Arg.LOG_EXT_FLIP) {
                                    System.out.println("EXT updated when rechecking "
                                            + transactionId + " on " + k + " at " + System.currentTimeMillis());
                                }
                            }
                            addANewOne = false;
                            break;
                        }
                    }
                    if (addANewOne) {
                        EXT<KeyType, ValueType> ext = new EXT<>(tidVal.getLeft(), this, k, tidVal.getRight(), v);
                        extViolations.add(ext);
                        if (Arg.LOG_EXT_FLIP) {
                            System.out.println("EXT created when rechecking "
                                    + transactionId + " on " + k + " at " + System.currentTimeMillis());
                        }
                    }
                } else {
                    // remove a potential EXT violation
                    Iterator<EXT<KeyType, ValueType>> it = extViolations.iterator();
                    while (it.hasNext()) {
                        EXT<KeyType, ValueType> extViolation = it.next();
                        if (extViolation.getKey().equals(k)) {
                            it.remove();
                            if (Arg.LOG_EXT_FLIP) {
                                System.out.println("EXT removed when rechecking "
                                        + transactionId + " on " + k + " at " + System.currentTimeMillis());
                            }
                        }
                    }
                }
            }
            intKeys.put(k, v);
        }
    }
}
