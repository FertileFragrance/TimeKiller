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

        String filepath = commandLine.getOptionValue("historyPath");
        System.out.println("Checking " + filepath);
        Arg.ENABLE_SESSION = Boolean.parseBoolean(commandLine.getOptionValue("enableSession", "true"));
        decideReader(filepath);

        Pair<? extends History<?, ?>, ArrayList<Violation>> historyAndViolations = reader.read(filepath);
        History<?, ?> history = historyAndViolations.getLeft();
        ArrayList<Violation> sessionViolations = historyAndViolations.getRight();

        if (Arg.ENABLE_SESSION) {
            if (sessionViolations.size() > 0) {
                System.out.println("Violate SESSION");
                sessionViolations.forEach(System.out::println);
            } else {
                System.out.println("Satisfy SESSION");
            }
        }
        ArrayList<Violation> violations = OneOffChecker.check(history);
        if (violations.size() > 0) {
            System.out.println("Do NOT satisfy SI");
            violations.forEach(System.out::println);
        } else {
            System.out.println("Satisfy SI");
        }

        long end = System.currentTimeMillis();
        System.out.println("Total time: " + (end - start) / 1000.0 + "s");
    }

    private static void setup(String[] args) {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("usage help").build());
        options.addOption(Option.builder("historyPath").required().hasArg(true)
                .type(String.class).desc("the filepath of execution history").build());
        options.addOption(Option.builder("enableSession").hasArg(true)
                .type(Boolean.class).desc("whether to check the SESSION axiom").build());
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp("TimeKiller", options, true);
            System.exit(1);
        }
    }

    private static void decideReader(String filepath) {
        File file = new File(filepath);
        if (!file.exists() || file.isDirectory()) {
            System.err.println("Invalid history path");
            System.exit(1);
        } else if (filepath.endsWith(".json")) {
            reader = new JSONFileReader();
        } else {
            System.err.println("Invalid history file suffix");
            System.exit(1);
        }
    }
}
