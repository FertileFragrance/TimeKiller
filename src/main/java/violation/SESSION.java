package violation;

import history.transaction.Transaction;

public class SESSION<KeyType, ValueType> extends Violation {
    private final Transaction<KeyType, ValueType> formerTxn;
    private final Transaction<KeyType, ValueType> latterTxn;
    private final String sessionId;

    public SESSION(Transaction<KeyType, ValueType> formerTxn, Transaction<KeyType, ValueType> latterTxn,
                   String sessionId) {
        this.type = ViolationType.SESSION;
        this.formerTxn = formerTxn;
        this.latterTxn = latterTxn;
        this.sessionId = sessionId;
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
