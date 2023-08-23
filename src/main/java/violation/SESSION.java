package violation;

import history.transaction.Transaction;

public class SESSION<KeyType, ValueType> extends Violation {
    private Transaction<KeyType, ValueType> formerTxn;
    private Transaction<KeyType, ValueType> latterTxn;
    private final String sessionId;

    public SESSION(Transaction<KeyType, ValueType> formerTxn, Transaction<KeyType, ValueType> latterTxn,
                   String sessionId) {
        this.type = ViolationType.SESSION;
        this.formerTxn = formerTxn;
        this.latterTxn = latterTxn;
        this.sessionId = sessionId;
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
        String s1 = "Violation of SESSION is found ";
        return s1 + "{" +
                "formerTxn=" + formerTxn +
                ", latterTxn=" + latterTxn +
                ", sessionId=" + sessionId +
                '}';
    }

    public Transaction<KeyType, ValueType> getFormerTxn() {
        return formerTxn;
    }

    public Transaction<KeyType, ValueType> getLatterTxn() {
        return latterTxn;
    }

    public String getSessionId() {
        return sessionId;
    }
}
