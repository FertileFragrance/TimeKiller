package history.transaction;

public class TransactionEntry<KeyType, ValueType> {
    private final Transaction<KeyType, ValueType> transaction;

    public enum EntryType {
        START, COMMIT
    }

    private final EntryType entryType;
    private final HybridLogicalClock timestamp;

    public TransactionEntry(Transaction<KeyType, ValueType> transaction,
                            EntryType entryType, HybridLogicalClock timestamp) {
        this.transaction = transaction;
        this.entryType = entryType;
        this.timestamp = timestamp;
    }

    public Transaction<KeyType, ValueType> getTransaction() {
        return transaction;
    }

    public EntryType getEntryType() {
        return entryType;
    }

    public HybridLogicalClock getTimestamp() {
        return timestamp;
    }
}
