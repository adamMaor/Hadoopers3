package univ.bigdata.course;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.Scanner;

public class MainRunner {

    public static void main(String[] args) throws FileNotFoundException {
        if (args[0].equals("commands")) {
            LinkedList<String> commands = returnFileLines("/home/vagrant/final-project/resources/" + args[1]);
            // first line is the input file
            String inputFile = commands.removeFirst();
            // second line is the output file
            String outputFile = commands.removeFirst();
            MovieQueriesProvider provider = new MovieQueriesProvider(inputFile);
            final PrintStream printer = initPrinter(outputFile);
            // following lines are the commands.
            for (String command : commands) {
                executeCommand(provider, printer, command);
            }
        }
    }

    /**
     * Returns a LinkedList of lines from a file.
     */
    private static LinkedList<String> returnFileLines(String path) throws FileNotFoundException {
        File inputFile = new File(path);
        Scanner inputScanner = new Scanner(inputFile);
        LinkedList<String> lines = new LinkedList<>();
        while (inputScanner.hasNextLine()) {
            lines.add(inputScanner.nextLine());
        }
        return lines;
    }

    /**
     * Executes a command on a given provider and outputs the output of the command to a printer.
     */
    private static void executeCommand(MovieQueriesProvider provider, PrintStream printer, String command) {
        String[] commandSplitted = command.split(" ");
        // function name is the first word, following words are parameters
        switch (commandSplitted[0]) {
            case "moviesCount":
                printer.println(provider.moviesCount());
                break;
            default:
                throw new RuntimeException("command not found " + commandSplitted[0]);
        }
    }

    /**
     * this static method initializes the printer.
     * it handles the FileNotFoundException
     * @param outputFile - the file to write to
     * @return the initialized printer
     */
    private static PrintStream initPrinter(String outputFile) {
        PrintStream printer = null;
        try {
            printer = new PrintStream(outputFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Output file open error");
        }
        return printer;
    }
}
