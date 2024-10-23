package checker.online;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import history.transaction.*;
import violation.EXT;
import violation.ViolationType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.zip.GZIPInputStream;

public class GcUtil {
    public static HashMap<String, String> tidEntryToFile;
    public static Kryo checkKryo;

    public static void init() {
        tidEntryToFile = new HashMap<>(2000);
        checkKryo = new Kryo();
        checkKryo.register(HashSet.class);
        checkKryo.register(TransactionEntry.class);
        checkKryo.register(Transaction.class);
        checkKryo.register(TransactionEntry.EntryType.class);
        checkKryo.register(HybridLogicalClock.class);
        checkKryo.register(Operation.class);
        checkKryo.register(OpType.class);
        checkKryo.register(HashMap.class);
        checkKryo.register(ArrayList.class);
        checkKryo.register(EXT.class);
        checkKryo.register(EXT.EXTType.class);
        checkKryo.register(ViolationType.class);
        checkKryo.register(TidVal.class);
        checkKryo.setReferences(true);
    }

    public static <KeyType, ValueType> TransactionEntry<KeyType, ValueType> readTxnEntry(String tidEntry) {
        String filepath = tidEntryToFile.get(tidEntry);
        try (Input input = new Input(new GZIPInputStream(Files.newInputStream(Paths.get(filepath))))) {
            HashSet<TransactionEntry<KeyType, ValueType>> txnEntries = checkKryo.readObject(input, HashSet.class);
            String tid = tidEntry.split("-")[0];
            String flag = tidEntry.split("-")[1];
            TransactionEntry.EntryType expected = TransactionEntry.EntryType.START;
            if ("c".equals(flag)) {
                expected = TransactionEntry.EntryType.COMMIT;
            }
            for (TransactionEntry<KeyType, ValueType> entry : txnEntries) {
                if (entry.getEntryType() == expected && entry.getTransaction().getTransactionId().equals(tid)) {
                    return entry;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static <KeyType, ValueType> Transaction<KeyType, ValueType> readTxn(String tid) {
        String filepath = tidEntryToFile.get(tid);
        try (Input input = new Input(new GZIPInputStream(Files.newInputStream(Paths.get(filepath))))) {
            HashSet<Transaction<KeyType, ValueType>> txns = checkKryo.readObject(input, HashSet.class);
            for (Transaction<KeyType, ValueType> txn : txns) {
                if (txn.getTransactionId().equals(tid)) {
                    return txn;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
