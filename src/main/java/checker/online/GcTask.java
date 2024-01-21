package checker.online;

import checker.OnlineChecker;
import history.History;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class GcTask implements Runnable {
    public static Lock gcLock = new ReentrantLock();

    private final OnlineChecker onlineChecker;
    private final History<?, ?> history;

    public GcTask(OnlineChecker onlineChecker, History<?, ?> history) {
        this.onlineChecker = onlineChecker;
        this.history = history;
    }

    @Override
    public void run() {
        while (true) {
            Pair<ArrayList<Integer>, ArrayList<Integer>> allAndInMemoryIndexes = onlineChecker.preGc(history);
            int gcSize = onlineChecker.serialize(allAndInMemoryIndexes, history);
            onlineChecker.doGc(allAndInMemoryIndexes, history, gcSize);
            System.gc();
        }
    }
}
