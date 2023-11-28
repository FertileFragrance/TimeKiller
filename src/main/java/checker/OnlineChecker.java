package checker;

import history.History;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;
import history.transaction.TransactionEntry;
import violation.EXT;
import violation.INT;
import violation.Violation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class OnlineChecker implements Checker {
    private final ArrayList<Violation> potentialViolations = new ArrayList<>();

    private final HashMap<Operation<?, ?>, EXT<?, ?>> incompleteExts = new HashMap<>(9);

    @Override
    public <KeyType, ValueType> ArrayList<Violation> check(History<KeyType, ValueType> history) {
        ArrayList<Violation> violations = new ArrayList<>();
        ArrayList<TransactionEntry<KeyType, ValueType>> txnEntries = history.getTransactionEntries();
        Transaction<KeyType, ValueType> currentTxn = txnEntries.get(history.getStartEntryIndex()).getTransaction();
        Transaction<KeyType, ValueType> lastCommittedTxn = null;
        for (int i = history.getStartEntryIndex() - 1; i >= 0; i--) {
            if (txnEntries.get(i).getEntryType() == TransactionEntry.EntryType.COMMIT) {
                lastCommittedTxn = txnEntries.get(i).getTransaction();
            }
        }
        assert lastCommittedTxn != null;
        int opSize = currentTxn.getOperations().size();
        HashMap<KeyType, ValueType> intKeys = new HashMap<>(opSize * 4 / 3 + 1);
        HashMap<KeyType, ValueType> extWriteKeys = new HashMap<>(opSize * 4 / 3 + 1);
        for (Operation<KeyType, ValueType> op : currentTxn.getOperations()) {
            KeyType k = op.getKey();
            ValueType v = op.getValue();
            if (op.getType() == OpType.read) {
                if (!intKeys.containsKey(k)) {
                    // TODO check EXT
                    Transaction<KeyType, ValueType> previousTxn;
                    if (lastCommittedTxn.getCommitFrontier().containsKey(k)) {
                        previousTxn = lastCommittedTxn.getCommitFrontier().get(k);
                    } else {
                        previousTxn = history.getInitialTxn();
                    }
                    if (!Objects.equals(previousTxn.getExtWriteKeys().get(k), v)) {
                        // TODO violate EXT
                        EXT<KeyType, ValueType> extViolation = new EXT<>(previousTxn,
                                currentTxn, k, previousTxn.getExtWriteKeys().get(k), v);
                        potentialViolations.add(extViolation);
                        incompleteExts.put(new Operation<>(OpType.write, k, v), extViolation);
                    }
                } else if (!Objects.equals(intKeys.get(k), v)) {
                    violations.add(new INT<>(currentTxn, k, intKeys.get(k), v));
                }
            } else {
                // TODO check incomplete ext?
                extWriteKeys.put(k, v);
            }
            intKeys.put(k, v);
        }
        currentTxn.setExtWriteKeys(extWriteKeys);
        HashMap<KeyType, Transaction<KeyType, ValueType>> commitFrontier =
                new HashMap<>(lastCommittedTxn.getCommitFrontier());
        for (KeyType k : extWriteKeys.keySet()) {
            commitFrontier.put(k, currentTxn);
        }
        currentTxn.setCommitFrontier(commitFrontier);
        // re-check EXT
        lastCommittedTxn = currentTxn;
        intKeys = new HashMap<>(opSize * 4 / 3 + 1);
        for (int i = history.getCommitEntryIndex() + 1; i < txnEntries.size(); i++) {
            // TODO if not timeout
            if (txnEntries.get(i).getEntryType() == TransactionEntry.EntryType.COMMIT) {
                lastCommittedTxn = txnEntries.get(i).getTransaction();
                continue;
            }
            Transaction<KeyType, ValueType> recheckTxn = txnEntries.get(i).getTransaction();
            for (Operation<KeyType, ValueType> op : recheckTxn.getOperations()) {
                KeyType k = op.getKey();
                ValueType v = op.getValue();
                if (op.getType() == OpType.read && !intKeys.containsKey(k)
                        && currentTxn.getExtWriteKeys().containsKey(k)) {
                    Transaction<KeyType, ValueType> previousTxn;
                    if (lastCommittedTxn.getCommitFrontier().containsKey(k)) {
                        previousTxn = lastCommittedTxn.getCommitFrontier().get(k);
                    } else {
                        previousTxn = history.getInitialTxn();
                    }
                    if (!Objects.equals(previousTxn.getExtWriteKeys().get(k), v)) {
                        EXT<KeyType, ValueType> extViolation = new EXT<>(previousTxn,
                                currentTxn, k, previousTxn.getExtWriteKeys().get(k), v);
                        potentialViolations.add(extViolation);
                        incompleteExts.put(new Operation<>(OpType.write, k, v), extViolation);
                    } else {
                        EXT<?, ?> extViolation = incompleteExts.get(op);
                        if (extViolation != null) {
                            incompleteExts.remove(op, extViolation);
                            potentialViolations.remove(extViolation);
                        }
                    }
                }
                intKeys.put(k, v);
            }
            // TODO else
            if (txnEntries.get(i).getEntryType() == TransactionEntry.EntryType.START) {
                continue;
            }
            commitFrontier = txnEntries.get(i).getTransaction().getCommitFrontier();
            for (KeyType k : currentTxn.getExtWriteKeys().keySet()) {
                if (!commitFrontier.containsKey(k)) {
                    commitFrontier.put(k, currentTxn);
                }
            }
        }
        return violations;
    }

    @Override
    public <KeyType, ValueType> void saveToFile(History<KeyType, ValueType> history) {

    }
}
