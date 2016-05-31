/**
 * Submitters information - Hadoopers team:
 * Vadim Khakham 	vadim.khakham@gmail.com	311890156
 * Michel Guralnik mikijoy@gmail.com 	306555822
 * Gilad Eini 	giladeini@gmail.com	034744920
 * Adam Maor 	maorcpa.adam@gmail.com	036930501
 */

package univ.bigdata.course;


import org.apache.spark.api.java.JavaPairRDD;
import scala.Serializable;
import scala.Tuple2;
import univ.bigdata.course.movie.Movie;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import univ.bigdata.course.movie.MovieReview;
import static java.lang.Math.toIntExact;

import java.util.Comparator;
import java.util.List;


public class MovieQueriesProvider implements Serializable{
    JavaRDD<MovieReview> movieReviews;
    /**
     * Constructor method
     */
    MovieQueriesProvider(String inputFile) {
        SparkConf conf = new SparkConf().setAppName("hw3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fileLines = sc.textFile("/home/vagrant/final-project/resources/" + inputFile);
        movieReviews = fileLines.map(MovieReview::new);
    }

    /**
     * Method which calculates total scores average for all movies.
     **/
    double totalMoviesAverageScore() {
        return totalMovieAverage("");
    }

    /**
     * For given movies calculates an average score of this movie.
     *
     * @param productId - id of the movie to calculate the average score.
     */
    double totalMovieAverage(final String productId) {
        JavaRDD<MovieReview> filteredMovieReviews = movieReviews.filter(s -> s.getMovie().getProductId().contains(productId));
        JavaRDD<Double> movieScores = filteredMovieReviews.map(s -> s.getMovie().getScore());
        double sum = movieScores.reduce((a,b) -> a+b);
        return roundFiveDecimal(sum/movieScores.count());
    }

    /**
     * For each movie calculates it's average score. List should be sorted
     * by average score in decreasing order and in case of same average tie
     * should be broken by natural order of product id. Only top k movies
     * with highest average should be returned
     *
     * @param topK - number of top movies to return
     */
    List<Movie> getTopKMoviesAverage(int topK) {
        return movieReviews
                .mapToPair(s-> new Tuple2<>(s.getMovie().getProductId(), new Tuple2<>(s.getMovie().getScore(), 1)))
                .reduceByKey((a, b)-> new Tuple2<>(a._1 + b._1, a._2 + b._2))
                .map(s -> new Movie(s._1, roundFiveDecimal(s._2._1 / s._2._2)))
                .top(getRealTopK(topK));
    }

    class StringDoubleTupleComparator implements Comparator<Tuple2<String, Double>>, Serializable {
        @Override
        public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
            if (o1._2().equals(o2._2())) {
                return o1._1().compareTo(o2._1());
            } else {
                return o1._2().compareTo(o2._2()) * -1;
            }
        }
    }

    /**
     * Finds movie with the highest average score among all available movies.
     * If more than one movie has same average score, then movie with lowest
     * product id ordered lexicographically.
     *
     * @return - the movies record @{@link Movie}
     */
    Movie movieWithHighestAverage() {
        return getTopKMoviesAverage(1).get(0);
    }

    /**
     * @return - the product id of most reviewed movie among all movies
     */
    String mostReviewedProduct() {
        List<Movie> list = reviewCountPerMovieTopKMovies(1);
        if (list.isEmpty()){
            return "No Movies Found !!!";
        }
        return list.get(0).getProductId();
    }

    /**
     * Computes reviews count per movie, sorted by reviews count in decreasing order, for movies
     * with same amount of review should be ordered by product id. Returns only top k movies with
     * highest review count.
     *
     * @return - returns map with movies product id and the count of over all reviews assigned to it.
     */
    List<Movie> reviewCountPerMovieTopKMovies(final int topK) {
        // we will "trick" movie to have the count instead of the score in order to use it's comperator
        return movieReviews
                .mapToPair(s -> new Tuple2<>(s.getMovie().getProductId(), 1))
                .reduceByKey((a, b) -> a + b)
                .mapToPair(s -> new Tuple2<>(new Movie(s._1(), (double)s._2()), s._2()))
                .keys().top(getRealTopK(topK));
    }

    /**
     * Computes most popular movie which has been reviewed by at least
     * numOfUsers (provided as parameter).
     *
     * @param numOfUsers - limit of minimum users which reviewed the movie
     * @return - movie which got highest count of reviews
     */
    String mostPopularMovieReviewedByKUsers(final int numOfUsers) {
        return null;
    }

    /**
     * Compute map of words count for top K words
     *
     * @param topK - top k number
     * @return - map where key is the word and value is the number of occurrences
     * of this word in the reviews summary, map ordered by words count in decreasing order.
     */
    String moviesReviewWordsCount(final int topK) {
        return null;
    }

    /**
     * Compute words count map for top Y most reviewed movies. Map includes top K
     * words.
     *
     * @param topMovies - number of top review movies
     * @param topWords - number of top words to return
     * @return - map of words to count, ordered by count in decreasing order.
     */
    String topYMoviesReviewTopXWordsCount(final int topMovies, final int topWords) {
        return null;
    }

    /**
     * Compute top k most helpful users that provided reviews for movies
     *
     * @param k - number of users to return
     * @return - map of users to number of reviews they made. Map ordered by number of reviews
     * in decreasing order.
     */
    String topKHelpfullUsers(final int k) {
        return null;
    }

    /**
     * Total movies count
     */
    long moviesCount() {
        JavaRDD<String> distinctMovies = movieReviews.map(s->s.getMovie().getProductId()).distinct();
        return distinctMovies.count();
    }

    /**
     * this service will check for topK if available and return the number
     * we really need to return
     *
     * @param k the top k given
     * @return the biggest possible
     */
    int getRealTopK(int k){
        long movieNum = moviesCount();
        return (int)(k > movieNum ? movieNum : k);
    }

    /**
     * This service method rounds a double to 5 decimal digits
     * @param number number to format
     * @return rounded double with 5 decimal points
     */
    static double roundFiveDecimal(double number) {
        return (double)Math.round((number) * 100000d) / 100000d;
    }
}
