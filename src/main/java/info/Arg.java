package info;

public class Arg {
    public static String FILEPATH;
    public static boolean ENABLE_SESSION = true;
    public static String INITIAL_VALUE = null;
    public static Long INITIAL_VALUE_LONG = null;
    public static String MODE = "fast";
    public static String CONSISTENCY_MODEL = "SI";
    public static String DATA_MODEL = "kv";
    public static boolean FIX = false;
    public static int NUM_PER_GC = 20000;
    public static int PORT = 23333;
    public static long TIMEOUT_DELAY = 5000L;
    public static boolean USE_CTS_AS_RTTS = false;
    public static long DURATION_IN_MEMORY = 10000L;
    public static boolean LOG_EXT_FLIP = false;
    public static int TXN_START_GC = 10000;
    public static int MAX_TXN_IN_MEM = 50000;
    public static long GC_INTERVAL = 10000L;
}
