package univ.bigdata.course;

import org.apache.spark.api.java.JavaDoubleRDD;
import scala.Tuple2;
import univ.bigdata.course.movie.Movie;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import univ.bigdata.course.movie.MovieReview;

import java.util.Comparator;
import java.util.List;

public class MovieQueriesProvider {
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
    String totalMoviesAverageScore() {
        JavaDoubleRDD movieScores = movieReviews.mapToDouble(s -> s.getMovie().getScore());
        return "Total average: " + roundFiveDecimal(movieScores.sum()/movieScores.count());
    }

    /**
     * For given movies calculates an average score of this movie.
     *
     * @param productId - id of the movie to calculate the average score.
     * @return - movie's average
     */
    String totalMovieAverage(final String productId) {
        return null;
    }

    /**
     * For each movie calculates it's average score. List should be sorted
     * by average score in decreasing order and in case of same average tie
     * should be broken by natural order of product id. Only top k movies
     * with highest average should be returned
     *
     * @param topK - number of top movies to return
     */
    List<Tuple2<String, Double>> getTopKMoviesAverage(int topK) {
        return movieReviews
                .mapToPair(s-> new Tuple2<>(s.getMovie().getProductId(), new Tuple2<>(s.getMovie().getScore(), 1)))
                .reduceByKey((a, b)-> new Tuple2<>(a._1 + b._1, a._2 + b._2))
                .mapToPair(s -> new Tuple2<>(new Tuple2<>(s._1, roundFiveDecimal(s._2._1 / s._2._2)), null))
                .sortByKey(tupleStringDoubleComparator, true)
                .mapToPair(Tuple2::_1)
                .take(topK);
    }

    private static Comparator<Tuple2<String, Double>> tupleStringDoubleComparator = (o1, o2) -> {
        if (o1._2().equals(o2._2())) {
            return o1._1().compareTo(o2._1());
        } else {
            return o1._2().compareTo(o2._2()) * -1;
        }
    };

    /**
     * Finds movie with the highest average score among all available movies.
     * If more than one movie has same average score, then movie with lowest
     * product id ordered lexicographically.
     *
     * @return - the movies record @{@link Movie}
     */
    String movieWithHighestAverage() {
        return null;
    }

    /**
     * Returns a list of movies which has average of given percentile.
     * List should be sorted according the average score of movie and in case there
     * are more than one movie with same average score, sort by product id
     * lexicographically in natural order.
     *
     * @param percent - the percentile, value in range between [0..100]
     * @return - movies list
     */
    String getMoviesPercentile(final double percent) {
        return null;
    }

    /**
     * @return - the product id of most reviewed movie among all movies
     */
    String mostReviewedProduct() {
        return null;
    }

    /**
     * Computes reviews count per movie, sorted by reviews count in decreasing order, for movies
     * with same amount of review should be ordered by product id. Returns only top k movies with
     * highest review count.
     *
     * @return - returns map with movies product id and the count of over all reviews assigned to it.
     */
    String reviewCountPerMovieTopKMovies(final int topK) {
        return null;
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
    String moviesCount() {
        JavaRDD<String> distinctMovies = movieReviews.map(s->s.getMovie().getProductId()).distinct();
        return "Total number of distinct movies reviewed [" + distinctMovies.count() + "].";
    }

    /**
     * This service method rounds a double to 5 decimal digits
     * @param number number to format
     * @return rounded double with 5 decimal points
     */
    private double roundFiveDecimal(double number)
    {
        return (double)Math.round((number) * 100000d) / 100000d;
    }
}
