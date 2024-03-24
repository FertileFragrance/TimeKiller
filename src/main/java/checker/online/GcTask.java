package checker.online;

import checker.OnlineChecker;
import history.History;
import info.Arg;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class GcTask implements Runnable {
    public static Lock gcLock = new ReentrantLock();
    public static volatile boolean doGc = false;
    public static volatile long nextGcTime = System.currentTimeMillis() + Arg.GC_INTERVAL;

    private final OnlineChecker onlineChecker;
    private final History<?, ?> history;

    public GcTask(OnlineChecker onlineChecker, History<?, ?> history) {
        this.onlineChecker = onlineChecker;
        this.history = history;

        GcUtil.init();
    }

    @Override
    public void run() {
        while (true) {
            if (doGc || history.getTransactionEntries().size() >= Arg.TXN_START_GC * 2
                    && System.currentTimeMillis() >= nextGcTime) {
                doGc = false;
                gcLock.lock();
                int size = onlineChecker.gc(history);
                gcLock.unlock();
                if (size > 0) {
                    System.gc();
                }
                nextGcTime = System.currentTimeMillis() + Arg.GC_INTERVAL;
            }
        }
    }
}
