/**
 * Submitters information - Hadoopers team:
 * Vadim Khakham 	vadim.khakham@gmail.com	311890156
 * Michel Guralnik mikijoy@gmail.com 	306555822
 * Gilad Eini 	giladeini@gmail.com	034744920
 * Adam Maor 	maorcpa.adam@gmail.com	036930501
 */

package univ.bigdata.course;

import univ.bigdata.course.movie.Movie;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.Scanner;

public class MainRunner {

    public static void main(String[] args) throws Exception {
        MovieQueriesProvider provider;
        MovieEvaluationProvider evaluation;
        PrintStream printer;
        LinkedList<String> commands;
        String inputFile, outputFile;
        switch (args[0]) {
            case "commands":
                commands = returnFileLines("/home/vagrant/final-project/resources/" + args[1]);
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
                commands = returnFileLines("/home/vagrant/final-project/resources/" + args[1]);
                // first line is the input file
                inputFile = commands.removeFirst();
                // second line is the output file
                outputFile = commands.removeFirst();
                evaluation = new MovieEvaluationProvider(inputFile);
                printer = initPrinter(outputFile);
                evaluation.getRecommendations(commands).forEach(printer::println);
                break;
            case "map":
                evaluation = new MovieEvaluationProvider(args[1], args[2]);
                System.out.println(evaluation.map());
                break;
            case "pagerank":
                provider = new MovieQueriesProvider(args[1]);
                printer = initPrinter("/home/vagrant/final-project/outputfile2.txt");
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
                printer.println("Total average for movie '" + commandSplitted[1] + "': " + provider.totalMovieAverage(commandSplitted[1]));
                break;
            case "getTopKMoviesAverage":
                provider.getTopKMoviesAverage(Integer.valueOf(commandSplitted[1])).forEach(printer::println);
                break;
            case "movieWithHighestAverage":
                Movie m1 = provider.movieWithHighestAverage();
                if (m1 != null) {
                    printer.println("The movie with highest average: " + m1);
                }
                else {
                    printer.println("Error! No movies in DB");
                }
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
                if (m2 != null) {
                    printer.println("Most popular movie with highest average score, reviewed by at least " + numOfUsers + " users " + m2.getProductId());
                }
                else {
                    printer.println("Error! No movies in DB");
                }
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
            printer = new PrintStream(outputFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Output file open error");
        }
        return printer;
    }
}
