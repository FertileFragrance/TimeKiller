package violation;

import history.transaction.Transaction;

public class NOCONFLICT<KeyType, ValueType> extends Violation {
    private final Transaction<KeyType, ValueType> formerTxn;
    private final Transaction<KeyType, ValueType> latterTxn;
    private final KeyType key;

    public NOCONFLICT(Transaction<KeyType, ValueType> formerTxn, Transaction<KeyType, ValueType> latterTxn,
                      KeyType key) {
        this.type = ViolationType.NOCONFLICT;
        this.formerTxn = formerTxn;
        this.latterTxn = latterTxn;
        this.key = key;
    }

    @Override
    public String toString() {
        String s1 = "Violation of NOCONFLICT is found ";
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
}
