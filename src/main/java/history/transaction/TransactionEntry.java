package history.transaction;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransactionEntry<?, ?> that = (TransactionEntry<?, ?>) o;
        return transaction.equals(that.transaction) && entryType == that.entryType && timestamp.equals(that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transaction, entryType, timestamp);
    }
}
