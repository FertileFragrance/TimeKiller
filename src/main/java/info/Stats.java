package info;

public class Stats {
    public static long TOTAL_START;
    public static long TOTAL_END;
    public static long LOADING_START;
    public static long LOADING_END;
    public static long SORTING_START;
    public static long SORTING_END;
    public static long CHECKING_START;
    public static long CHECKING_END;
    private static long GC_TIME = 0;

    public static void addGcTime(long start, long end) {
        GC_TIME += end - start;
    }

    public static void printTimeUsage() {
        System.out.println("=========[ Time Usage Statistics ]=========");
        System.out.printf("|  Total time:              %-10f s  |\n", (TOTAL_END - TOTAL_START) / 1000.0);
        System.out.printf("|  Loading time:            %-10f s  |\n", (LOADING_END - LOADING_START) / 1000.0);
        System.out.printf("|  Sorting time:            %-10f s  |\n", (SORTING_END - SORTING_START) / 1000.0);
        if ("gc".equals(Arg.MODE)) {
            System.out.printf("|  Pure checking time:      %-10f s  |\n", (CHECKING_END - CHECKING_START - GC_TIME) / 1000.0);
            System.out.printf("|  Garbage collecting time: %-10f s  |\n", GC_TIME / 1000.0);
        } else if ("fast".equals(Arg.MODE)) {
            System.out.printf("|  Checking time:           %-10f s  |\n", (CHECKING_END - CHECKING_START) / 1000.0);
        }
        System.out.println("===========================================\n");
    }
}
