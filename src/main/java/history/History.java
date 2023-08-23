package history;

import history.transaction.Transaction;
import history.transaction.TransactionEntry;

import java.util.*;

public class History<KeyType, ValueType> {
    private final ArrayList<Transaction<KeyType, ValueType>> transactions;
    private final ArrayList<TransactionEntry<KeyType, ValueType>> transactionEntries;
    private final HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> keyWritten;

    public History(ArrayList<Transaction<KeyType, ValueType>> transactions,
                   ArrayList<TransactionEntry<KeyType, ValueType>> transactionEntries,
                   HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> keyWritten) {
        this.transactions = transactions;
        this.transactionEntries = transactionEntries;
        this.keyWritten = keyWritten;
        if (this.transactions != null) {
            this.transactions.sort(Comparator.comparing(Transaction::getCommitTimestamp));
        }
        if (this.transactionEntries != null) {
            Collections.sort(this.transactionEntries);
        }
    }

    public void reset() {
        if (transactions != null) {
            transactions.sort(Comparator.comparing(Transaction::getCommitTimestamp));
        }
        if (transactionEntries != null) {
            transactionEntries.forEach(e -> {
                if (e.getEntryType() == TransactionEntry.EntryType.START) {
                    e.setTimestamp(e.getTransaction().getStartTimestamp());
                } else {
                    e.setTimestamp(e.getTransaction().getCommitTimestamp());
                }
            });
            Collections.sort(transactionEntries);
        }
        for (ArrayList<Transaction<KeyType, ValueType>> writeToKeyTxns : keyWritten.values()) {
            ListIterator<Transaction<KeyType, ValueType>> iterator = writeToKeyTxns.listIterator(writeToKeyTxns.size());
            while(iterator.hasPrevious()){
                iterator.previous();
                if (iterator.nextIndex() > 0) {
                    iterator.remove();
                }
            }
        }
    }

    public ArrayList<Transaction<KeyType, ValueType>> getTransactions() {
        return transactions;
    }

    public ArrayList<TransactionEntry<KeyType, ValueType>> getTransactionEntries() {
        return transactionEntries;
    }

    public HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> getKeyWritten() {
        return keyWritten;
    }
}
