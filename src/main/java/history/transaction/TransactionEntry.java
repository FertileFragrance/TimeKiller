package history.transaction;

import java.io.Serializable;
import java.util.Objects;

public class TransactionEntry<KeyType, ValueType> implements Comparable<TransactionEntry<KeyType, ValueType>>,
        Serializable {
    @Override
    public int compareTo(TransactionEntry o) {
        int diff = timestamp.compareTo(o.timestamp);
        if (diff != 0) {
            return diff;
        }
        if (transaction.getTransactionId().equals(o.transaction.getTransactionId())) {
            if (entryType == EntryType.START) {
                return -1;
            } else {
                return 1;
            }
        }
        if (entryType == EntryType.COMMIT && o.entryType == EntryType.START) {
            return -1;
        }
        if (entryType == EntryType.START && o.entryType == EntryType.COMMIT) {
            return 1;
        }
        return 0;
    }

    private Transaction<KeyType, ValueType> transaction;

    public enum EntryType implements Serializable {
        START, COMMIT
    }

    private EntryType entryType;
    private HybridLogicalClock timestamp;

    public TransactionEntry(Transaction<KeyType, ValueType> transaction,
                            EntryType entryType, HybridLogicalClock timestamp) {
        this.transaction = transaction;
        this.entryType = entryType;
        this.timestamp = timestamp;
    }

    public TransactionEntry() {}

    public Transaction<KeyType, ValueType> getTransaction() {
        return transaction;
    }

    public EntryType getEntryType() {
        return entryType;
    }

    public HybridLogicalClock getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(HybridLogicalClock timestamp) {
        this.timestamp = timestamp;
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
