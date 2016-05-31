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
import java.util.List;
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
                Movie highestAvg = provider.movieWithHighestAverage();
                printer.println("The movie with highest average: " + highestAvg);
                break;
            case "mostReviewedProduct":
                printer.println("The most reviewed movie product id is " + provider.mostReviewedProduct());
                break;
            case "reviewCountPerMovieTopKMovies":
                List<Movie> movies = provider.reviewCountPerMovieTopKMovies(Integer.parseInt(commandSplitted[1]));
                for (Movie movie : movies) {
                    printer.println("Movie product id = [" + movie.getProductId()+ "], reviews count [" + movie.getScore() + "].");
                }
                ;
                break;
            case "mostPopularMovieReviewedByKUsers":
                printer.println(provider.mostPopularMovieReviewedByKUsers(Integer.parseInt(commandSplitted[1])));
                break;
            case "moviesReviewWordsCount":
                printer.println(provider.moviesReviewWordsCount(Integer.parseInt(commandSplitted[1])));
                break;
            case "topYMoviesReviewTopXWordsCount":
                printer.println(provider.topYMoviesReviewTopXWordsCount(Integer.parseInt(commandSplitted[1]),
                        Integer.parseInt(commandSplitted[2])));
                break;
            case "topKHelpfullUsers":
                printer.println(provider.topKHelpfullUsers(Integer.parseInt(commandSplitted[1])));
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
        PrintStream printer = null;
        try {
            printer = new PrintStream(outputFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Output file open error");
        }
        return printer;
    }
}
