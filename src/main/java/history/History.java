package history;

import history.transaction.Transaction;

import java.util.*;

public class History<KeyType, ValueType> {
    private final ArrayList<Transaction<KeyType, ValueType>> transactions;
    private final HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> keyWritten;

    public History(ArrayList<Transaction<KeyType, ValueType>> transactions,
                   HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> keyWritten) {
        this.transactions = transactions;
        this.keyWritten = keyWritten;
        this.transactions.sort(Comparator.comparing(Transaction::getCommitTimestamp));
    }

    public ArrayList<Transaction<KeyType, ValueType>> getTransactions() {
        return transactions;
    }

    public HashMap<KeyType, ArrayList<Transaction<KeyType, ValueType>>> getKeyWritten() {
        return keyWritten;
    }
}
