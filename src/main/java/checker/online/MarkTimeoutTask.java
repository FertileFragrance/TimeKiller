package checker.online;

import history.transaction.Transaction;

import java.util.TimerTask;

public class MarkTimeoutTask extends TimerTask {
    private final Transaction<?, ?> txn;

    public MarkTimeoutTask(Transaction<?, ?> txn) {
        this.txn = txn;
    }

    @Override
    public void run() {
        txn.markTimeout();
    }
}
