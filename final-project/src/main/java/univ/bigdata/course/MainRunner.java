/**
 * Submitters information - Hadoopers team:
 * Vadim Khakham 	vadim.khakham@gmail.com	311890156
 * Michel Guralnik mikijoy@gmail.com 	306555822
 * Gilad Eini 	giladeini@gmail.com	034744920
 * Adam Maor 	maorcpa.adam@gmail.com	036930501
 */

package univ.bigdata.course;

import univ.bigdata.course.movie.Movie;

import java.io.*;
import java.util.LinkedList;
import java.util.Scanner;

public class MainRunner {

    public static void main(String[] args) throws Exception {
        MovieQueriesProvider provider;
        MovieEvaluationProvider evaluation;
        PrintStream printer;
        LinkedList<String> commands;
        String inputFile, outputFile;
        // check the arg[0] and act accordingly
        switch (args[0]) {
            case "commands":
                commands = returnFileLines(args[1]);
                // first line is the input file
                inputFile = commands.removeFirst();
                // second line is the output file
                outputFile = commands.removeFirst();
                provider = new MovieQueriesProvider(inputFile);
                printer = initPrinter(outputFile);
                // following lines are the commands.
                for (String command : commands) {
                    if (!command.isEmpty()) {
                        executeCommand(provider, printer, command);
                    }
                }
                break;
            case "recommend":
                commands = returnFileLines(args[1]);
                // first line is the input file
                inputFile = commands.removeFirst();
                // second line is the output file
                outputFile = commands.removeFirst();
                evaluation = new MovieEvaluationProvider(inputFile);
                printer = initPrinter(outputFile);
                evaluation.getRecommendations(commands).forEach(s->printer.println(s
                        + "======================================"));
                break;
            case "map":
                printer = initPrinter("");
                evaluation = new MovieEvaluationProvider(args[1], args[2]);
                printer.println("Map score: " + evaluation.map());
                break;
            case "pagerank":
                provider = new MovieQueriesProvider(args[1]);
                printer = initPrinter("");
                provider.getPageRank().forEach(printer::println);
                break;
            default:
                throw new RuntimeException("command not found " + args[0]);
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
        printer.println(command);
        switch (commandSplitted[0]) {
            case "totalMoviesAverageScore":
                printer.println("Total average: " + provider.totalMoviesAverageScore());
                break;
            case "totalMovieAverage":
                printer.println("Movies " + commandSplitted[1] + " average is " + provider.totalMovieAverage(commandSplitted[1]));
                break;
            case "getTopKMoviesAverage":
                provider.getTopKMoviesAverage(Integer.valueOf(commandSplitted[1])).forEach(printer::println);
                break;
            case "movieWithHighestAverage":
                Movie m1 = provider.movieWithHighestAverage();
                printer.println("The movie with highest average:  " + (m1 != null ? m1 : ""));
                break;
            case "mostReviewedProduct":
                printer.println("The most reviewed movie product id is " + provider.mostReviewedProduct());
                break;
            case "reviewCountPerMovieTopKMovies":
                provider.reviewCountPerMovieTopKMovies(Integer.parseInt(commandSplitted[1])).forEach(printer::println);
                break;
            case "mostPopularMovieReviewedByKUsers":
                Integer numOfUsers = Integer.parseInt(commandSplitted[1]);
                Movie m2 = provider.mostPopularMovieReviewedByKUsers(numOfUsers);
                printer.println("Most popular movie with highest average score, reviewed by at least " + numOfUsers
                        + " users " + (m2 != null ? m2.getProductId() : ""));
                break;
            case "moviesReviewWordsCount":
                provider.moviesReviewWordsCount(Integer.parseInt(commandSplitted[1])).forEach(printer::println);
                break;
            case "topYMoviesReviewTopXWordsCount":
                provider.topYMoviesReviewTopXWordsCount(Integer.parseInt(commandSplitted[1]),
                        Integer.parseInt(commandSplitted[2])).forEach(printer::println);
                break;
            case "topKHelpfullUsers":
                provider.topKHelpfullUsers(Integer.parseInt(commandSplitted[1])).forEach(printer::println);
                break;
            case "moviesCount":
                printer.println("Total number of distinct movies reviewed [" + provider.moviesCount()  + "].");
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
        PrintStream printer;
        try {
            if (outputFile.isEmpty())
            {
                printer = new PrintStream(new FileOutputStream(FileDescriptor.out));
            }
            else
            {
                printer = new PrintStream(outputFile);
            }
        } catch (Exception e) {
            throw new RuntimeException("Cannot open printer to std::out");
        }
        return printer;
    }
}
