import checker.OnlineChecker;
import checker.online.GcTask;
import checker.online.HttpRequest;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sun.net.httpserver.HttpServer;
import info.*;
import checker.Checker;
import checker.FastChecker;
import checker.GcChecker;
import history.History;
import org.apache.commons.io.FileUtils;
import reader.JSONFileGcReader;
import reader.OnlineReader;
import violation.Violation;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.Pair;
import reader.JSONFileFastReader;
import reader.Reader;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class TimeKiller {
    private static Reader<?, ?> reader;
    private static Checker checker;

    public static void main(String[] args) throws IOException {
        Stats.TOTAL_START = System.currentTimeMillis();

        setup(args);
        decideReaderAndChecker();

        if ("online".equals(Arg.MODE)) {
            runOnlineMode();
            System.exit(0);
        }

        System.out.println("Checking " + Arg.FILEPATH);
        Pair<? extends History<?, ?>, ArrayList<Violation>> historyAndViolations = reader.read(Arg.FILEPATH);
        History<?, ?> history = historyAndViolations.getLeft();
        ArrayList<Violation> sessionViolations = historyAndViolations.getRight();
        printSessionViolations(sessionViolations);

        Stats.CHECKING_START = System.currentTimeMillis();
        ArrayList<Violation> violations = checker.check(history);
        Stats.CHECKING_END = System.currentTimeMillis();
        printSiViolations(violations);

        violations.addAll(sessionViolations);
        postCheck(history, violations);

        Stats.TOTAL_END = System.currentTimeMillis();
        Stats.printTimeUsage();
    }

    private static void setup(String[] args) {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("print usage help and exit").build());
        options.addOption(Option.builder().longOpt("history_path").hasArg(true).type(String.class)
                .desc("the filepath of history in json format").build());
        options.addOption(Option.builder().longOpt("enable_session").hasArg(true).type(Boolean.class)
                .desc("whether to check the SESSION axiom using timestamps [default: true]").build());
        options.addOption(Option.builder().longOpt("initial_value").hasArg(true).type(Long.class)
                .desc("the initial value of keys before all writes [default: null]").build());
        options.addOption(Option.builder().longOpt("mode").hasArg(true).type(String.class)
                .desc("choose a mode to run TimeKiller [default: fast] [possible values: fast, gc]").build());
        options.addOption(Option.builder().longOpt("fix").desc("fix violations if found").build());
        options.addOption(Option.builder().longOpt("num_per_gc").hasArg(true).type(Integer.class)
                .desc("the number of checked transactions for each gc [default: 20000]").build());
        options.addOption(Option.builder().longOpt("port").hasArg(true).type(Integer.class)
                .desc("HTTP request port for online checking [default: 23333]").build());
        options.addOption(Option.builder().longOpt("timeout_delay").hasArg(true).type(Long.class)
                .desc("transaction timeout delay of online checking in millisecond [default: 5000]").build());
        options.addOption(Option.builder().longOpt("use_cts_as_rtts").hasArg(true).type(Boolean.class)
                .desc("use the physical part of commit timestamp as realtime timestamp [default: false]").build());
        options.addOption(Option.builder().longOpt("duration_in_memory").hasArg(true).type(Integer.class)
                .desc("the duration transaction kept in memory in realtime [default: 10000]").build());
        try {
            CommandLine commandLine = parser.parse(options, args);
            if (commandLine.hasOption("h")) {
                printAndExit(options);
            }
            Arg.MODE = commandLine.getOptionValue("mode", "fast");
            if (!"fast".equals(Arg.MODE) && !"gc".equals(Arg.MODE) && !"online".equals(Arg.MODE)) {
                System.out.println("Arg for --mode is invalid");
                printAndExit(options);
            }
            Arg.FILEPATH = commandLine.getOptionValue("history_path", "default");
            if ("default".equals(Arg.FILEPATH) && !"online".equals(Arg.MODE)) {
                System.out.println("--history_path is required for fast or gc mode");
                printAndExit(options);
            }
            Arg.ENABLE_SESSION = Boolean.parseBoolean(commandLine.getOptionValue("enable_session", "true"));
            Arg.INITIAL_VALUE = commandLine.getOptionValue("initial_value", null);
            if (Arg.INITIAL_VALUE != null && !"null".equalsIgnoreCase(Arg.INITIAL_VALUE)) {
                Arg.INITIAL_VALUE_LONG = Long.parseLong(Arg.INITIAL_VALUE);
            }
            if (commandLine.hasOption("fix")) {
                Arg.FIX = true;
            }
            if (commandLine.hasOption("num_per_gc")) {
                Arg.NUM_PER_GC = Integer.parseInt(commandLine.getOptionValue("num_per_gc"));
            }
            if (commandLine.hasOption("port")) {
                Arg.PORT = Integer.parseInt(commandLine.getOptionValue("port"));
            }
            if (commandLine.hasOption("timeout_delay")) {
                Arg.TIMEOUT_DELAY = Long.parseLong(commandLine.getOptionValue("timeout_delay"));
            }
            if (commandLine.hasOption("use_cts_as_rtts")) {
                Arg.USE_CTS_AS_RTTS = Boolean.parseBoolean(commandLine.getOptionValue("use_cts_as_rtts"));
            }
            if (commandLine.hasOption("duration_in_memory")) {
                Arg.DURATION_IN_MEMORY = Long.parseLong(commandLine.getOptionValue("duration_in_memory"));
            }
        } catch (ParseException e) {
            printAndExit(options);
        }
    }

    private static void printAndExit(Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp(120, "TimeKiller", null, options, null, true);
        System.exit(0);
    }

    private static void printSessionViolations(ArrayList<Violation> sessionViolations) {
        if (Arg.ENABLE_SESSION) {
            if (!sessionViolations.isEmpty()) {
                System.out.println("Do NOT satisfy SESSION");
                sessionViolations.forEach(System.out::println);
                if (sessionViolations.size() == 1) {
                    System.out.println("Total 1 violation of SESSION is found");
                } else {
                    System.out.println("Total " + sessionViolations.size() + " violations of SESSION are found");
                }
            } else {
                System.out.println("Satisfy SESSION");
            }
        }
    }

    private static void printSiViolations(ArrayList<Violation> violations) {
        if (!violations.isEmpty()) {
            System.out.println("Do NOT satisfy SI");
            violations.forEach(System.out::println);
            if (violations.size() == 1) {
                System.out.println("Total 1 violation of SI is found");
            } else {
                System.out.println("Total " + violations.size() + " violations of SI are found");
            }
        } else {
            System.out.println("Satisfy SI");
        }
    }

    private static void decideReaderAndChecker() {
        if ("online".equals(Arg.MODE)) {
            reader = new OnlineReader();
            checker = new OnlineChecker();
            return;
        }
        File file = new File(Arg.FILEPATH);
        if (!file.exists() || file.isDirectory()) {
            System.err.println("Invalid history path");
            System.exit(1);
        } else if (!Arg.FILEPATH.endsWith(".json")) {
            System.err.println("Invalid history file suffix");
            System.exit(1);
        } else {
            if ("fast".equals(Arg.MODE)) {
                reader = new JSONFileFastReader();
                checker = new FastChecker();
            } else if ("gc".equals(Arg.MODE)) {
                reader = new JSONFileGcReader();
                checker = new GcChecker();
            }
        }
    }

    private static void postCheck(History<?, ?> history, ArrayList<Violation> violations) {
        if (Arg.FIX) {
            fix(history, violations);
        }
    }

    private static void fix(History<?, ?> history, ArrayList<Violation> violations) {
        if (violations.isEmpty()) {
            System.out.println("No need to fix");
            return;
        }
        violations.forEach(Violation::fix);
        System.out.println("After fixing for the first time...");
        history.reset();
        violations = checker.check(history);
        printSiViolations(violations);
        if (!violations.isEmpty()) {
            violations.forEach(Violation::fix);
            System.out.println("After fixing for the second time...");
            history.reset();
            violations = checker.check(history);
            printSiViolations(violations);
            if (!violations.isEmpty()) {
                System.out.println("Fixing failed unexpectedly");
                return;
            }
        }
        checker.saveToFile(history);
    }

    private static void runOnlineMode() throws IOException {
        // create cache directory
        String programPath = System.getProperty("user.dir");
        File cacheDir = new File(programPath + "/.cache/TimeKiller/");
        if (cacheDir.exists()) {
            FileUtils.deleteDirectory(cacheDir);
        }
        if (!cacheDir.mkdirs()) {
            System.err.println("Create cache directory error!");
            System.exit(1);
        }
        // init history
        History<?, ?> history = reader.read(null).getLeft();
        // set GC
        Thread gcThread = new Thread(new GcTask((OnlineChecker) checker, history));
        gcThread.start();
        // start HTTP server
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        BlockingQueue<HttpRequest> requestQueue = new LinkedBlockingQueue<>();
        HttpServer server = HttpServer.create(new InetSocketAddress(Arg.PORT), 0);
        server.createContext("/check", exchange -> {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            try (InputStream requestBody = exchange.getRequestBody(); OutputStream os = exchange.getResponseBody()) {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int length;
                while ((length = requestBody.read(buffer)) != -1) {
                    bytes.write(buffer, 0, length);
                }
                HttpRequest request = new HttpRequest(bytes.toString());
                requestQueue.put(request);
                String response = "Request received and added to the queue";
                exchange.sendResponseHeaders(200, response.length());
                os.write(response.getBytes());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        server.setExecutor(executorService);
        server.start();
        System.out.println("Server started on port " + Arg.PORT);
        // handle HTTP request
        while (true) {
            try {
                HttpRequest request = requestQueue.take();
                if ("STOP".equals(request.getContent())) {
                    server.stop(1000);
                    executorService.shutdownNow();
                    FileUtils.deleteDirectory(cacheDir);
                    break;
                }
                JSONArray jsonArray = JSONArray.parseArray(request.getContent());
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                    if (history.getTransactionEntries().size() > 5000) {
                        Thread.sleep(100);
                    }
                    GcTask.gcLock.lock();
                    try {
                        Pair<? extends History<?, ?>, ArrayList<Violation>> historyAndViolations = reader.read(jsonObject);
                        ArrayList<Violation> violations = historyAndViolations.getRight();
                        violations.forEach(System.out::println);
                        violations = checker.check(history);
                        violations.forEach(System.out::println);
                    } finally {
                        GcTask.gcLock.unlock();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
