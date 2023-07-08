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
