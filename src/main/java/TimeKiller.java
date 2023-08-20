import arg.Arg;
import checker.OneOffChecker;
import history.History;
import violation.Violation;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.Pair;
import reader.JSONFileReader;
import reader.Reader;

import java.io.File;
import java.util.ArrayList;

public class TimeKiller {
    private static CommandLine commandLine;
    private static Reader<?, ?> reader;

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        setup(args);

        Arg.FILEPATH = commandLine.getOptionValue("history_path");
        System.out.println("Checking " + Arg.FILEPATH);
        Arg.ENABLE_SESSION = Boolean.parseBoolean(commandLine.getOptionValue("enable_session", "true"));
        decideReader();

        Arg.INITIAL_VALUE = commandLine.getOptionValue("initial_value", null);
        if (Arg.INITIAL_VALUE != null && !"null".equalsIgnoreCase(Arg.INITIAL_VALUE)) {
            Arg.INITIAL_VALUE_LONG = Long.parseLong(Arg.INITIAL_VALUE);
        }

        Pair<? extends History<?, ?>, ArrayList<Violation>> historyAndViolations = reader.read(Arg.FILEPATH);
        History<?, ?> history = historyAndViolations.getLeft();
        ArrayList<Violation> sessionViolations = historyAndViolations.getRight();

        if (Arg.ENABLE_SESSION) {
            if (sessionViolations.size() > 0) {
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
        ArrayList<Violation> violations = OneOffChecker.check(history);
        if (violations.size() > 0) {
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

        long end = System.currentTimeMillis();
        System.out.println("Total time: " + (end - start) / 1000.0 + "s");
    }

    private static void setup(String[] args) {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("print usage help and exit").build());
        options.addOption(Option.builder().longOpt("history_path").required().hasArg(true)
                .type(String.class).desc("the filepath of history in json format").build());
        options.addOption(Option.builder().longOpt("enable_session").hasArg(true)
                .type(Boolean.class).desc("whether to check the SESSION axiom [default: true]").build());
        options.addOption(Option.builder().longOpt("initial_value").hasArg(true)
                .type(Long.class).desc("the initial value of keys before all writes [default: null]").build());
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption("h")) {
                HelpFormatter hf = new HelpFormatter();
                hf.printHelp(92, "TimeKiller", null, options, null, true);
                System.exit(0);
            }
        } catch (ParseException e) {
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp(92, "TimeKiller", null, options, null, true);
            System.exit(1);
        }
    }

    private static void decideReader() {
        File file = new File(Arg.FILEPATH);
        if (!file.exists() || file.isDirectory()) {
            System.err.println("Invalid history path");
            System.exit(1);
        } else if (Arg.FILEPATH.endsWith(".json")) {
            reader = new JSONFileReader();
        } else {
            System.err.println("Invalid history file suffix");
            System.exit(1);
        }
    }
}
