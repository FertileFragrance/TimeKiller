package violation;

import info.Arg;
import history.transaction.Transaction;

public class NOCONFLICT<KeyType, ValueType> extends Violation {
    private Transaction<KeyType, ValueType> formerTxn;
    private Transaction<KeyType, ValueType> latterTxn;
    private final KeyType key;

    public NOCONFLICT(Transaction<KeyType, ValueType> formerTxn, Transaction<KeyType, ValueType> latterTxn,
                      KeyType key) {
        this.type = ViolationType.NOCONFLICT;
        this.formerTxn = formerTxn;
        this.latterTxn = latterTxn;
        this.key = key;
    }

    @Override
    public void fix() {
        if (formerTxn.getCommitTimestamp().compareTo(latterTxn.getCommitTimestamp()) > 0) {
            Transaction<KeyType, ValueType> tmp = formerTxn;
            formerTxn = latterTxn;
            latterTxn = tmp;
        }
        latterTxn.setStartTimestamp(formerTxn.getCommitTimestamp());
    }

    @Override
    public String toString() {
        String s1 = "Violation of NOCONFLICT is found ";
        String s2;
        if ("initial".equals(formerTxn.getTransactionId())) {
            s2 = "{id=initial, ops=[w(" + key + ", " + Arg.INITIAL_VALUE + ")], startTs=HLC(0, 0), commitTs=HLC(0, 0)}";
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
