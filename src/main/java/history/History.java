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
            this.transactionEntries.sort(Comparator.comparing(TransactionEntry::getTimestamp));
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
