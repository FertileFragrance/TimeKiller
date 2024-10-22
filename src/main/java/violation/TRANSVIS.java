package violation;

import history.transaction.Transaction;

public class TRANSVIS<KeyType, ValueType> extends Violation {
    private Transaction<KeyType, ValueType> formerTxn;
    private Transaction<KeyType, ValueType> latterTxn;

    public TRANSVIS(Transaction<KeyType, ValueType> formerTxn, Transaction<KeyType, ValueType> latterTxn) {
        this.type = ViolationType.TRANSVIS;
        this.formerTxn = formerTxn;
        this.latterTxn = latterTxn;
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
        String s1 = "Violation of TRANSVIS is found ";
        return s1 + "{" +
                "formerTxn=" + formerTxn +
                ", latterTxn=" + latterTxn +
                '}';
    }

    public Transaction<KeyType, ValueType> getFormerTxn() {
        return formerTxn;
    }

    public Transaction<KeyType, ValueType> getLatterTxn() {
        return latterTxn;
    }
}
