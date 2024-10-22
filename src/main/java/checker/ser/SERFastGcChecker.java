package checker.ser;

import checker.Checker;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import history.History;
import history.transaction.OpType;
import history.transaction.Operation;
import history.transaction.Transaction;
import info.Arg;
import info.Stats;
import org.apache.commons.lang3.tuple.Pair;
import violation.EXT;
import violation.INT;
import violation.TRANSVIS;
import violation.Violation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;

public class SERFastGcChecker implements Checker {
    @Override
    public <KeyType, ValueType> ArrayList<Violation> check(History<KeyType, ValueType> history) {
        ArrayList<Violation> violations = new ArrayList<>();
        HashMap<Operation<KeyType, ValueType>, EXT<KeyType, ValueType>> incompleteExts = new HashMap<>(9);
        HashMap<KeyType, Pair<String, ValueType>> frontierTidVal = history.getFrontierTidVal();
        int checkedTxnCount = 0;
        for (int i = 1; i < history.getTransactions().size(); i++) {
            Transaction<KeyType, ValueType> currentTxn = history.getTransactions().get(i);
            // check TRANSVIS
            Transaction<KeyType, ValueType> lastCommittedTxn = history.getTransactions().get(i - 1);
            if (currentTxn.getStartTimestamp().compareTo(lastCommittedTxn.getCommitTimestamp()) < 0) {
                violations.add(new TRANSVIS<>(lastCommittedTxn, currentTxn));
            }
            int opSize = currentTxn.getOperations().size();
            HashMap<KeyType, ValueType> intKeys = new HashMap<>(opSize * 4 / 3 + 1);
            HashMap<KeyType, ValueType> extWriteKeys = new HashMap<>(opSize * 4 / 3 + 1);
            for (Operation<KeyType, ValueType> op : currentTxn.getOperations()) {
                KeyType k = op.getKey();
                ValueType v = op.getValue();
                if (op.getType() == OpType.read) {
                    if (!intKeys.containsKey(k)) {
                        // check EXT
                        if (!Objects.equals(frontierTidVal.get(k).getRight(), v)) {
                            // violate EXT
                            EXT<KeyType, ValueType> extViolation = new EXT<>(frontierTidVal.get(k).getLeft(),
                                    currentTxn, k, frontierTidVal.get(k).getRight(), v);
                            violations.add(extViolation);
                            if (Objects.equals(history.getInitialTxn().getExtWriteKeys().get(k), v)) {
                                extViolation.setWriteLatterValueTxn(history.getInitialTxn());
                                extViolation.setExtType(EXT.EXTType.BEFORE);
                            }
                            if (extViolation.getExtType() == EXT.EXTType.NEVER) {
                                for (int j = i - 1; j >= 1; j--) {
                                    Transaction<KeyType, ValueType> writeTxn = history.getTransactions().get(j);
                                    if (Objects.equals(writeTxn.getExtWriteKeys().get(k), v)) {
                                        extViolation.setWriteLatterValueTxn(writeTxn);
                                        extViolation.setExtType(EXT.EXTType.BEFORE);
                                        break;
                                    }
                                }
                            }
                            if (extViolation.getExtType() == EXT.EXTType.NEVER) {
                                incompleteExts.put(new Operation<>(OpType.write, k, v), extViolation);
                            }
                        }
                    } else if (!Objects.equals(intKeys.get(k), v)) {
                        // violate INT
                        violations.add(new INT<>(currentTxn, k, intKeys.get(k), v));
                    }
                } else {
                    EXT<KeyType, ValueType> extViolation = incompleteExts.get(op);
                    if (extViolation != null) {
                        extViolation.setWriteLatterValueTxn(currentTxn);
                        extViolation.setExtType(EXT.EXTType.AFTER);
                        incompleteExts.remove(op, extViolation);
                    }
                    extWriteKeys.put(k, v);
                    frontierTidVal.put(k, Pair.of(currentTxn.getTransactionId(), v));
                }
                intKeys.put(k, v);
            }
            currentTxn.setExtWriteKeys(extWriteKeys);
            // perform optional gc
            checkedTxnCount++;
            if ("gc".equals(Arg.MODE) && checkedTxnCount == Arg.NUM_PER_GC) {
                long gcStart = System.currentTimeMillis();
                i = gc(history, i);
                System.gc();
                Stats.addGcTime(gcStart, System.currentTimeMillis());
                checkedTxnCount = 0;
            }
        }
        return violations;
    }

    public <KeyType, ValueType> int gc(History<KeyType, ValueType> history, int currentIndex) {
        HashSet<Transaction<KeyType, ValueType>> toRemove = new HashSet<>(currentIndex * 4 / 3 + 1);
        ArrayList<Transaction<KeyType, ValueType>> txns = history.getTransactions();
        for (int i = 1; i < currentIndex; i++) {
            toRemove.add(txns.get(i));
        }
        Transaction<KeyType, ValueType> currentTxn = txns.get(currentIndex);
        txns.removeAll(toRemove);
        return txns.indexOf(currentTxn);
    }

    @Override
    public <KeyType, ValueType> void saveToFile(History<KeyType, ValueType> history) {
        ArrayList<Transaction<KeyType, ValueType>> transactions = history.getTransactions();
        ArrayList<Transaction<KeyType, ValueType>> txns = new ArrayList<>(transactions.size() - 1);
        for (int i = 1; i < transactions.size(); i++) {
            txns.add(transactions.get(i));
        }
        String content = JSON.toJSONString(txns, SerializerFeature.DisableCircularReferenceDetect);
        File file = new File(Arg.FILEPATH);
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(
                    new File(Arg.FILEPATH).getParent() + File.separator + "FIXED-" + file.getName()));
            out.write(content);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
