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
