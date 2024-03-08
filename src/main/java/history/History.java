package history;

import history.transaction.Transaction;
import history.transaction.TransactionEntry;
import info.Arg;
import info.Stats;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class History<KeyType, ValueType> {
    private final ArrayList<Transaction<KeyType, ValueType>> transactions;
    private final ArrayList<TransactionEntry<KeyType, ValueType>> transactionEntries;
    private final HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> keyWritten;
    private final int keyNumber;

    private final HashMap<KeyType, ValueType> frontierVal;
    private final HashMap<KeyType, String> frontierTid;

    private final Transaction<KeyType, ValueType> initialTxn;

    private final ArrayList<Pair<String, Boolean>> txnIdWhetherGc = new ArrayList<>(2);
    private int startEntryIndex;
    private int commitEntryIndex;
    private int startEntryIndexInMemory;
    private int commitEntryIndexInMemory;

    public History(ArrayList<Transaction<KeyType, ValueType>> transactions, int keyNumber,
                   ArrayList<TransactionEntry<KeyType, ValueType>> transactionEntries,
                   HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> keyWritten,
                   HashMap<KeyType, ValueType> frontierVal, HashMap<KeyType, String> frontierTid) {
        this.transactions = transactions;
        this.keyNumber = keyNumber;
        this.transactionEntries = transactionEntries;
        this.keyWritten = keyWritten;
        this.frontierVal = frontierVal;
        this.frontierTid = frontierTid;

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
            txnIdWhetherGc.add(Pair.of("initial-s", false));
            txnIdWhetherGc.add(Pair.of("initial-c", false));
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
            frontierVal.forEach((k, v) -> frontierVal.put(k, initialTxn.getOperations().get(0).getValue()));
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

    public HashMap<KeyType, ValueType> getFrontierVal() {
        return frontierVal;
    }

    public HashMap<KeyType, String> getFrontierTid() {
        return frontierTid;
    }

    public Transaction<KeyType, ValueType> getInitialTxn() {
        return initialTxn;
    }

    public ArrayList<Pair<String, Boolean>> getTxnIdWhetherGc() {
        return txnIdWhetherGc;
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

    public int getStartEntryIndexInMemory() {
        return startEntryIndexInMemory;
    }

    public void setStartEntryIndexInMemory(int startEntryIndexInMemory) {
        this.startEntryIndexInMemory = startEntryIndexInMemory;
    }

    public int getCommitEntryIndexInMemory() {
        return commitEntryIndexInMemory;
    }

    public void setCommitEntryIndexInMemory(int commitEntryIndexInMemory) {
        this.commitEntryIndexInMemory = commitEntryIndexInMemory;
    }
}
