package history;

import history.transaction.Transaction;
import history.transaction.TransactionEntry;

import java.util.*;

public class History<KeyType, ValueType> {
    private final ArrayList<Transaction<KeyType, ValueType>> transactions;
    private final ArrayList<TransactionEntry<KeyType, ValueType>> transactionEntries;
    private final HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> keyWritten;
    private final int keyNumber;
    private final HashMap<KeyType, Transaction<KeyType, ValueType>> frontier;
    private final Transaction<KeyType, ValueType> initialTxn;

    public History(ArrayList<Transaction<KeyType, ValueType>> transactions, int keyNumber,
                   ArrayList<TransactionEntry<KeyType, ValueType>> transactionEntries,
                   HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> keyWritten,
                   HashMap<KeyType, Transaction<KeyType, ValueType>> frontier) {
        this.transactions = transactions;
        this.keyNumber = keyNumber;
        this.transactionEntries = transactionEntries;
        this.keyWritten = keyWritten;
        this.frontier = frontier;
        if (this.transactions != null) {
            this.transactions.sort(Comparator.comparing(Transaction::getCommitTimestamp));
            this.initialTxn = this.transactions.get(0);
        } else {
            // this.transactionEntries != null
            Collections.sort(this.transactionEntries);
            this.initialTxn = this.transactionEntries.get(0).getTransaction();
        }
    }

    public void reset() {
        if (transactions != null) {
            transactions.sort(Comparator.comparing(Transaction::getCommitTimestamp));
            for (ArrayList<Transaction<KeyType, ValueType>> writeToKeyTxns : keyWritten.values()) {
                ListIterator<Transaction<KeyType, ValueType>> it = writeToKeyTxns.listIterator(writeToKeyTxns.size());
                while (it.hasPrevious()) {
                    it.previous();
                    if (it.nextIndex() > 0) {
                        it.remove();
                    }
                }
            }
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
            frontier.forEach((k, v) -> frontier.put(k, initialTxn));
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

    public int getKeyNumber() {
        return keyNumber;
    }

    public HashMap<KeyType, Transaction<KeyType, ValueType>> getFrontier() {
        return frontier;
    }

    public Transaction<KeyType, ValueType> getInitialTxn() {
        return initialTxn;
    }
}
