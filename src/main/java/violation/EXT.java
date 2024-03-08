package violation;

import info.Arg;
import history.transaction.Operation;
import history.transaction.Transaction;

import java.io.Serializable;

public class EXT<KeyType, ValueType> extends Violation implements Serializable {
    private String formerTxnId;
    private final Transaction<KeyType, ValueType> latterTxn;
    private final KeyType key;
    private ValueType formerValue;
    private final ValueType latterValue;
    private Transaction<KeyType, ValueType> writeLatterValueTxn;

    public enum EXTType implements Serializable {
        NEVER, BEFORE, UNCOMMITTED, AFTER
    }

    private EXTType extType;

    public EXT(String formerTxnId, Transaction<KeyType, ValueType> latterTxn,
               KeyType key, ValueType formerValue, ValueType latterValue) {
        this.type = ViolationType.EXT;
        this.formerTxnId = formerTxnId;
        this.latterTxn = latterTxn;
        this.key = key;
        this.formerValue = formerValue;
        this.latterValue = latterValue;
        this.extType = EXTType.NEVER;
    }

    @Override
    public void fix() {
        for (Operation<KeyType, ValueType> operation : latterTxn.getOperations()) {
            if (!key.equals(operation.getKey())) {
                continue;
            }
            operation.setValue(formerValue);
            break;
        }
    }

    @Override
    public String toString() {
        String s1 = "Violation of EXT is found ";
        String s2;
        if (writeLatterValueTxn == null) {
            s2 = "null";
        } else if ("initial".equals(writeLatterValueTxn.getTransactionId())) {
            s2 = "{id=initial, ops=[w(" + key + ", " + Arg.INITIAL_VALUE + ")], startTs=HLC(0, 0), commitTs=HLC(0, 0)}";
        } else {
            s2 = writeLatterValueTxn.toString();
        }
        String result = s1 + "{" +
                "formerTxnId=" + formerTxnId +
                ", latterTxn=" + latterTxn +
                ", key=" + key +
                ", formerValue=" + formerValue +
                ", latterValue=" + latterValue;
        if ("online".equals(Arg.MODE)) {
            result += '}';
        } else {
            result += ", writeLatterValueTxn=" + s2 +
                    ", extType=" + extType +
                    '}';
        }
        return result;
    }

    public String getFormerTxnId() {
        return formerTxnId;
    }

    public void setFormerTxnId(String formerTxnId) {
        this.formerTxnId = formerTxnId;
    }

    public Transaction<KeyType, ValueType> getLatterTxn() {
        return latterTxn;
    }

    public KeyType getKey() {
        return key;
    }

    public ValueType getFormerValue() {
        return formerValue;
    }

    public void setFormerValue(ValueType formerValue) {
        this.formerValue = formerValue;
    }

    public ValueType getLatterValue() {
        return latterValue;
    }

    public Transaction<KeyType, ValueType> getWriteLatterValueTxn() {
        return writeLatterValueTxn;
    }

    public void setWriteLatterValueTxn(Transaction<KeyType, ValueType> writeLatterValueTxn) {
        this.writeLatterValueTxn = writeLatterValueTxn;
    }

    public EXTType getExtType() {
        return extType;
    }

    public void setExtType(EXTType extType) {
        this.extType = extType;
    }
}
