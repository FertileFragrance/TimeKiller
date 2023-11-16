package violation;

import info.Arg;
import history.transaction.Operation;
import history.transaction.Transaction;

public class EXT<KeyType, ValueType> extends Violation {
    private final Transaction<KeyType, ValueType> formerTxn;
    private final Transaction<KeyType, ValueType> latterTxn;
    private final KeyType key;
    private final ValueType formerValue;
    private final ValueType latterValue;
    private Transaction<KeyType, ValueType> writeLatterValueTxn;

    public enum EXTType {
        NEVER, BEFORE, UNCOMMITTED, AFTER
    }

    private EXTType extType;

    public EXT(Transaction<KeyType, ValueType> formerTxn, Transaction<KeyType, ValueType> latterTxn,
               KeyType key, ValueType formerValue, ValueType latterValue) {
        this.type = ViolationType.EXT;
        this.formerTxn = formerTxn;
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
        if ("initial".equals(formerTxn.getTransactionId())) {
            s2 = "{id=initial, ops=[w(" + key + ", " + Arg.INITIAL_VALUE + ")], startTs=HLC(0, 0), commitTs=HLC(0, 0)}";
        } else {
            s2 = formerTxn.toString();
        }
        String s3;
        if (writeLatterValueTxn == null) {
            s3 = "null";
        } else if ("initial".equals(writeLatterValueTxn.getTransactionId())) {
            s3 = "{id=initial, ops=[w(" + key + ", " + Arg.INITIAL_VALUE + ")], startTs=HLC(0, 0), commitTs=HLC(0, 0)}";
        } else {
            s3 = writeLatterValueTxn.toString();
        }
        return s1 + "{" +
                "formerTxn=" + s2 +
                ", latterTxn=" + latterTxn +
                ", key=" + key +
                ", formerValue=" + formerValue +
                ", latterValue=" + latterValue +
                ", writeLatterValueTxn=" + s3 +
                ", extType=" + extType +
                '}';
    }

    public Transaction<KeyType, ValueType> getFormerTxn() {
        return formerTxn;
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
