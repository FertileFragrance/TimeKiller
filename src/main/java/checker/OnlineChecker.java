package checker;

import checker.online.MarkTimeoutTask;
import history.History;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;
import history.transaction.TransactionEntry;
import info.Arg;
import violation.EXT;
import violation.INT;
import violation.Violation;

import java.util.*;

public class OnlineChecker implements Checker {
    private final Timer timer = new Timer();

    @Override
    public <KeyType, ValueType> ArrayList<Violation> check(History<KeyType, ValueType> history) {
        ArrayList<Violation> violations = new ArrayList<>();
        ArrayList<TransactionEntry<KeyType, ValueType>> txnEntries = history.getTransactionEntries();
        Transaction<KeyType, ValueType> currentTxn = txnEntries.get(history.getStartEntryIndex()).getTransaction();
        Transaction<KeyType, ValueType> lastCommittedTxn = null;
        for (int i = history.getStartEntryIndex() - 1; i >= 0; i--) {
            if (txnEntries.get(i).getEntryType() == TransactionEntry.EntryType.COMMIT) {
                lastCommittedTxn = txnEntries.get(i).getTransaction();
                break;
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
                    // check EXT
                    Transaction<KeyType, ValueType> previousTxn = lastCommittedTxn.getCommitFrontier()
                            .getOrDefault(k, history.getInitialTxn());
                    if (!Objects.equals(previousTxn.getExtWriteKeys().get(k), v)) {
                        // violate EXT so far
                        currentTxn.getExtViolations().add(new EXT<>(previousTxn,
                                currentTxn, k, previousTxn.getExtWriteKeys().get(k), v));
                    }
                } else if (!Objects.equals(intKeys.get(k), v)) {
                    violations.add(new INT<>(currentTxn, k, intKeys.get(k), v));
                }
            } else {
                extWriteKeys.put(k, v);
            }
            intKeys.put(k, v);
        }
        currentTxn.setExtWriteKeys(extWriteKeys);
        HashMap<KeyType, Transaction<KeyType, ValueType>> commitFrontier =
                new HashMap<>(lastCommittedTxn.getCommitFrontier());
        for (int i = history.getStartEntryIndex() + 1; i < history.getCommitEntryIndex(); i++) {
            if (txnEntries.get(i).getEntryType() == TransactionEntry.EntryType.COMMIT) {
                for (KeyType k : txnEntries.get(i).getTransaction().getExtWriteKeys().keySet()) {
                    commitFrontier.put(k, txnEntries.get(i).getTransaction());
                }
            }
        }
        for (KeyType k : extWriteKeys.keySet()) {
            commitFrontier.put(k, currentTxn);
        }
        currentTxn.setCommitFrontier(commitFrontier);
        // re-check EXT
        lastCommittedTxn = currentTxn;
        HashSet<KeyType> writeKeys = new HashSet<>(currentTxn.getExtWriteKeys().keySet());
        for (int i = history.getCommitEntryIndex() + 1; i < txnEntries.size(); i++) {
            Transaction<KeyType, ValueType> recheckTxn = txnEntries.get(i).getTransaction();
            if (txnEntries.get(i).getEntryType() == TransactionEntry.EntryType.START) {
                recheckTxn.recheckExt(lastCommittedTxn, currentTxn, history.getInitialTxn());
            } else {
                lastCommittedTxn = recheckTxn;
                writeKeys.removeIf(k -> recheckTxn.getExtWriteKeys().containsKey(k));
                if (writeKeys.isEmpty()) {
                    break;
                }
                commitFrontier = recheckTxn.getCommitFrontier();
                for (KeyType k : writeKeys) {
                    commitFrontier.put(k, currentTxn);
                }
            }
        }
        timer.schedule(new MarkTimeoutTask(currentTxn), Arg.TIMEOUT_DELAY);
        return violations;
    }

    @Override
    public <KeyType, ValueType> void saveToFile(History<KeyType, ValueType> history) {

    }
}
