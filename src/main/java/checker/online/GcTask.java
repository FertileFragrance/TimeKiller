package checker.online;

import checker.OnlineChecker;
import history.History;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class GcTask implements Runnable {
    public static Lock gcLock = new ReentrantLock();

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
            gcLock.lock();
            int size = onlineChecker.gc(history);
            gcLock.unlock();
            if (size > 50) {
                System.gc();
            } else {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
