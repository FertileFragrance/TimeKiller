import arg.Arg;
import checker.Checker;
import checker.FastChecker;
import checker.GcChecker;
import history.History;
import reader.JSONFileGcReader;
import violation.Violation;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.Pair;
import reader.JSONFileFastReader;
import reader.Reader;

import java.io.File;
import java.util.ArrayList;

public class TimeKiller {
    private static Reader<?, ?> reader;
    private static Checker checker;

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        setup(args);
        decideReaderAndChecker();

        Pair<? extends History<?, ?>, ArrayList<Violation>> historyAndViolations = reader.read(Arg.FILEPATH);
        History<?, ?> history = historyAndViolations.getLeft();
        ArrayList<Violation> sessionViolations = historyAndViolations.getRight();
        printSessionViolations(sessionViolations);
        ArrayList<Violation> violations = checker.check(history);
        printSiViolations(violations);

        violations.addAll(sessionViolations);
        postCheck(history, violations);

        long end = System.currentTimeMillis();
        System.out.println("Total time: " + (end - start) / 1000.0 + "s");
    }

    private static void setup(String[] args) {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("print usage help and exit").build());
        options.addOption(Option.builder().longOpt("history_path").required().hasArg(true).type(String.class)
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
        try {
            CommandLine commandLine = parser.parse(options, args);
            if (commandLine.hasOption("h")) {
                printAndExit(options);
            }
            Arg.MODE = commandLine.getOptionValue("mode", "fast");
            if (!"fast".equals(Arg.MODE) && !"gc".equals(Arg.MODE)) {
                System.out.println("Arg for --mode is invalid");
                printAndExit(options);
            }
            Arg.FILEPATH = commandLine.getOptionValue("history_path");
            System.out.println("Checking " + Arg.FILEPATH);
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
            } else {
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
}
