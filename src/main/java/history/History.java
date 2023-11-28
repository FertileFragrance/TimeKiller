package history;

import history.transaction.Transaction;
import history.transaction.TransactionEntry;
import info.Arg;
import info.Stats;

import java.util.*;

public class History<KeyType, ValueType> {
    private final ArrayList<Transaction<KeyType, ValueType>> transactions;
    private final ArrayList<TransactionEntry<KeyType, ValueType>> transactionEntries;
    private final HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> keyWritten;
    private final int keyNumber;
    private final HashMap<KeyType, Transaction<KeyType, ValueType>> frontier;
    private final Transaction<KeyType, ValueType> initialTxn;

    private int startEntryIndex;
    private int commitEntryIndex;

    public History(ArrayList<Transaction<KeyType, ValueType>> transactions, int keyNumber,
                   ArrayList<TransactionEntry<KeyType, ValueType>> transactionEntries,
                   HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> keyWritten,
                   HashMap<KeyType, Transaction<KeyType, ValueType>> frontier) {
        this.transactions = transactions;
        this.keyNumber = keyNumber;
        this.transactionEntries = transactionEntries;
        this.keyWritten = keyWritten;
        this.frontier = frontier;

        Stats.SORTING_START = System.currentTimeMillis();

        if ("fast".equals(Arg.MODE)) {
            this.transactions.sort(Comparator.comparing(Transaction::getCommitTimestamp));
            this.initialTxn = this.transactions.get(0);
        } else if ("gc".equals(Arg.MODE)) {
            Collections.sort(this.transactionEntries);
            this.initialTxn = this.transactionEntries.get(0).getTransaction();
        } else {
            // Arg.MODE is online
            this.initialTxn = this.transactionEntries.get(0).getTransaction();
        }

        Stats.SORTING_END = System.currentTimeMillis();
    }

    public void reset() {
        if ("fast".equals(Arg.MODE)) {
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
        } else if ("gc".equals(Arg.MODE)) {
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

    public int getStartEntryIndex() {
        return startEntryIndex;
    }

    public void setStartEntryIndex(int startEntryIndex) {
        this.startEntryIndex = startEntryIndex;
    }

    public int getCommitEntryIndex() {
        return commitEntryIndex;
    }

    public void setCommitEntryIndex(int commitEntryIndex) {
        this.commitEntryIndex = commitEntryIndex;
    }
}
