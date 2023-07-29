package violation;

import history.transaction.Transaction;

public class EXT<KeyType, ValueType> extends Violation {
    private final Transaction<KeyType, ValueType> formerTxn;
    private final Transaction<KeyType, ValueType> latterTxn;
    private final KeyType key;
    private final ValueType formerValue;
    private final ValueType latterValue;

    public EXT(Transaction<KeyType, ValueType> formerTxn, Transaction<KeyType, ValueType> latterTxn,
               KeyType key, ValueType formerValue, ValueType latterValue) {
        this.type = ViolationType.EXT;
        this.formerTxn = formerTxn;
        this.latterTxn = latterTxn;
        this.key = key;
        this.formerValue = formerValue;
        this.latterValue = latterValue;
    }

    @Override
    public String toString() {
        String s1 = "Violation of EXT is found ";
        String s2;
        if ("initial".equals(formerTxn.getTransactionId())) {
            s2 = "{id=initial, ops=[w(" + key + ", null)], startTs=HLC(0, 0), commitTs=HLC(0, 0)}";
        } else {
            s2 = formerTxn.toString();
        }
        return s1 + "{" +
                "formerTxn=" + s2 +
                ", latterTxn=" + latterTxn +
                ", key=" + key +
                ", formerValue=" + formerValue +
                ", latterValue=" + latterValue +
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
}
