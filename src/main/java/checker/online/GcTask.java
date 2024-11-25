package checker.online;

import checker.OnlineChecker;
import history.History;
import history.transaction.HybridLogicalClock;
import info.Arg;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class GcTask implements Runnable {
    public static Lock gcLock = new ReentrantLock(true);
    public static volatile boolean doGc = false;
    public static volatile long nextGcTime = System.currentTimeMillis() + Arg.GC_INTERVAL;

    private final OnlineChecker onlineChecker;
    private final History<?, ?> history;

    public GcTask(OnlineChecker onlineChecker, History<?, ?> history) {
        this.onlineChecker = onlineChecker;
        this.history = history;

        GcUtil.init();
    }

    public static volatile HybridLogicalClock maxTimestampInRemove = new HybridLogicalClock(0L, 0L);

    public static boolean judgeFull(History<?, ?> history) {
        if ("SI".equals(Arg.CONSISTENCY_MODEL)) {
            return history.getTransactionEntries().size() >= Arg.MAX_TXN_IN_MEM * 2;
        } else {
            return history.getTransactions().size() >= Arg.MAX_TXN_IN_MEM;
        }
    }

    public static boolean judgeDoGc(History<?, ?> history) {
        if ("SI".equals(Arg.CONSISTENCY_MODEL)) {
            if (history.getTransactionEntries().size() >= Arg.TXN_START_GC * 2
                    || history.getTransactionEntries().size() >= Arg.MAX_TXN_IN_MEM * 2) {
                doGc = true;
                return true;
            } else {
                return false;
            }
        } else {
            if (history.getTransactions().size() >= Arg.TXN_START_GC
                    || history.getTransactions().size() >= Arg.MAX_TXN_IN_MEM) {
                doGc = true;
                return true;
            } else {
                return false;
            }
        }
    }

//    @Override
    public void run1() {
        while (true) {
            int size;
            if ("SI".equals(Arg.CONSISTENCY_MODEL)) {
                size = history.getTransactionEntries().size() / 2;
            } else {
                size = history.getTransactions().size();
            }
            if (doGc || size >= Arg.TXN_START_GC && System.currentTimeMillis() >= nextGcTime) {
                doGc = false;
                gcLock.lock();
                int removedSize = onlineChecker.gc(history);
                gcLock.unlock();
                if (removedSize > 0) {
                    System.gc();
                }
                nextGcTime = System.currentTimeMillis() + Arg.GC_INTERVAL;
            }
        }
    }

    @Override
    public void run() {
        while (true) {
            int size;
            if ("SI".equals(Arg.CONSISTENCY_MODEL)) {
                size = history.getTransactionEntries().size() / 2;
            } else {
                size = history.getTransactions().size();
            }
            if (doGc || judgeFull(history) || size >= Arg.TXN_START_GC && System.currentTimeMillis() >= nextGcTime) {
                doGc = false;
                gcLock.lock();
                onlineChecker.preGc(history);
                gcLock.unlock();
                onlineChecker.performGc();
                gcLock.lock();
                int removedSize = onlineChecker.postGc(history);
                gcLock.unlock();
                if (removedSize > 0) {
                    System.gc();
                }
                nextGcTime = System.currentTimeMillis() + Arg.GC_INTERVAL;
            }
        }
    }
}
