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
import univ.bigdata.course.movie.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import univ.bigdata.course.movie.MovieReview;
import univ.bigdata.course.movie.PersonPageRank;
import univ.bigdata.course.movie.WordCount;
import univ.bigdata.course.movie.MovieCountedReview;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MovieQueriesProvider implements Serializable{
    JavaRDD<MovieReview> movieReviews;
    /**
     * Constructor method
     */
    MovieQueriesProvider(String inputFile) {
        SparkConf conf = new SparkConf().setAppName("hw3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // create our main line RDD from input file
        // it will be used in all queries
        JavaRDD<String> fileLines = sc.textFile(inputFile);
        movieReviews = fileLines.map(MovieReview::new);
    }

    /**
     * Method which calculates total scores average for all movies.
     **/
    double totalMoviesAverageScore() {
        // call totalMovieAverage with no movie name
        // so filter will filter nothing and we'll get all the movies
        return totalMovieAverage("");
    }

    /**
     * For given movies calculates an average score of this movie.
     *
     * @param productId - id of the movie to calculate the average score.
     */
    double totalMovieAverage(final String productId) {
        // filter by movie ID - if "" than all movies
        JavaRDD<MovieReview> filteredMovieReviews = movieReviews.filter(s -> s.getMovie().getProductId().contains(productId));
        // check validity
        if (filteredMovieReviews.isEmpty()){
            return 0;
        }
        // get movie scores from reviews
        JavaRDD<Double> movieScores = filteredMovieReviews.map(s -> s.getMovie().getScore());
        // sum the scores
        double sum = movieScores.reduce((a,b) -> a+b);
        // divide sum by count and return
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
        List<Movie> list = new ArrayList<Movie>();
        // check k validity
        int k = getRealTopK(topK, moviesCount());
        if ( k > 0) {
            // map to pairs of pID and and a pair of movie score and 1
            // then reduce by key to count scores and reviews together
            // then map to pairs of pID and AVG
            // and get the topK from this RDD to a list (collect)
            list =  movieReviews
                    .mapToPair(s-> new Tuple2<>(s.getMovie().getProductId(), new Tuple2<>(s.getMovie().getScore(), 1)))
                    .reduceByKey((a, b)-> new Tuple2<>(a._1 + b._1, a._2 + b._2))
                    .map(s -> new Movie(s._1, roundFiveDecimal(s._2._1 / s._2._2)))
                    .top(k);
        }
        return list;
    }

    /**
     * Finds movie with the highest average score among all available movies.
     * If more than one movie has same average score, then movie with lowest
     * product id ordered lexicographically.
     *
     * @return - the movies record @{@link Movie}
     */
    Movie movieWithHighestAverage() {
        // simply call topK with 1
        List<Movie> list = getTopKMoviesAverage(1);
        if (list.size() > 0){
            return list.get(0);
        }
        return null;
    }

    /**
     * @return - the product id of most reviewed movie among all movies
     */
    String mostReviewedProduct() {
        // simply call topK with 1
        List<MovieCountedReview> list = reviewCountPerMovieTopKMovies(1);
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
    List<MovieCountedReview> reviewCountPerMovieTopKMovies(final int topK) {

        List<MovieCountedReview> list = new ArrayList<>();
        // check k validity
        int k = getRealTopK(topK, moviesCount());
        if (k > 0) {
            // map to pair of pID and 1
            // then reduce by key to count the reviews
            // then map to an RDD of a new CLASS that has a review comparator of it's own
            // get the topK from the RDD (collect)
            list = movieReviews
                    .mapToPair(s -> new Tuple2<>(s.getMovie().getProductId(), 1))
                    .reduceByKey((a, b) -> a + b)
                    .map(s -> new MovieCountedReview(s._1(), s._2()))
                    .top(k);
        }
        return list;
    }

    /**
     * Computes most popular movie which has been reviewed by at least
     * numOfUsers (provided as parameter).
     *
     * @param numOfUsers - limit of minimum users which reviewed the movie
     * @return - movie which got highest count of reviews
     */
    Movie mostPopularMovieReviewedByKUsers(final int numOfUsers) {
        // similar to getTopKMoviesAverage only filter by numOfUsers to be as specified
        List<Movie> popularMovieFiltered =  movieReviews
            .mapToPair(s-> new Tuple2<>(s.getMovie().getProductId(), new Tuple2<>(s.getMovie().getScore(), 1)))
            .reduceByKey((a, b)-> new Tuple2<>(a._1 + b._1, a._2 + b._2))
            .filter(s -> s._2._2 >= numOfUsers)
            .map(s -> new Movie(s._1, roundFiveDecimal(s._2._1 / s._2._2)))
            .top(1);
        // return null if empty
        return popularMovieFiltered.isEmpty()? null : popularMovieFiltered.get(0);
    }

    /**
     * Compute map of words count for top K words
     *
     * @param topK - top k number
     */
    List<WordCount> moviesReviewWordsCount(final int topK) {
        // get the review, split by " " , map to pair with 1, reduce by key to count
        // then map to WordCount CLASS the has it's own comparator and get the topK (validity checked) - (collect)
        JavaRDD<WordCount> wordCounts = movieReviews
                .map(MovieReview::getReview)
                .flatMap(s-> Arrays.asList(s.split(" ")))
                .mapToPair(s->new Tuple2<>(s, 1))
                .reduceByKey((a, b)-> a + b)
                .map(s->new WordCount(s._1(), s._2()));
        return wordCounts.top(getRealTopK(topK, wordCounts.count()));
    }

    /**
     * Compute words count map for top Y most reviewed movies. Map includes top K
     * words.
     *
     * @param topMovies - number of top review movies
     * @param topWords - number of top words to return
     * @return - map of words to count, ordered by count in decreasing order.
     */
    List<WordCount> topYMoviesReviewTopXWordsCount(final int topMovies, final int topWords) {
        List<String> topYMovies = new ArrayList<>();
        reviewCountPerMovieTopKMovies(topMovies).forEach(movie -> topYMovies.add(movie.getProductId()));
        // similar to moviesReviewWordsCount only filtered by pID
        JavaRDD<WordCount> wordCounts = movieReviews
                .filter(s-> topYMovies.contains(s.getMovie().getProductId()))
                .map(MovieReview::getReview)
                .flatMap(s-> Arrays.asList(s.split(" ")))
                .mapToPair(s->new Tuple2<>(s, 1))
                .reduceByKey((a, b)-> a + b)
                .map(s->new WordCount(s._1(), s._2()));
        return wordCounts.top(getRealTopK(topWords, wordCounts.count()));
    }

    /**
     * Compute top k most helpful users that provided reviews for movies
     *
     * @param k - number of users to return
     * @return - map of users to number of reviews they made. Map ordered by number of reviews
     * in decreasing order.
     */
    List<PersonHelpfulness> topKHelpfullUsers(final int k) {
        // map to pairs of uID and helpfulness, then cut the helpfulness to 2 parts
        // filter out 0s, then sum both parts using reduce by key
        // then, map to PersonHelpfulness CLASS that has it's own comparator
        // then, return topK (validity checked) list (collect)
        JavaRDD<PersonHelpfulness> helpfulUsers = movieReviews
            .mapToPair(a -> new Tuple2<>(a.getUserId(), a.getHelpfulness()))
            .mapToPair(s -> new Tuple2<>(s._1, new Tuple2<>(Double.parseDouble(s._2.substring(0,s._2.lastIndexOf('/'))),
                    Double.parseDouble(s._2.substring(s._2.lastIndexOf('/') + 1, s._2.length())))))
            .filter(s -> s._2._2 != 0)
            .reduceByKey((a,b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2))
            .map(s -> new PersonHelpfulness(s._1, roundFiveDecimal(s._2._1/s._2._2)));
        return helpfulUsers.top(getRealTopK(k,helpfulUsers.count() ));
    }

    /**
     * Total movies count
     */
    long moviesCount() {
        return movieReviews
                .map(s->s.getMovie().getProductId())
                .distinct()
                .count();
    }

    /**
     * create a graph for PageRank
     * @return the result of PageRank
     * @throws Exception
     */
    List<PersonPageRank> getPageRank() throws Exception {
        // first map to pair of pID and uID and keep only distinct
        JavaPairRDD<String, String> graph = movieReviews
                .mapToPair(s->new Tuple2<>(s.getMovie().getProductId(), s.getUserId())).distinct();
        // now, join graph to graph (by pId) and filter out those with the same uID
        // then, map to " " divided string of pID and uID and keep only distinct pairs
        JavaRDD<String> graphCart =
                graph.join(graph)
                .filter(s->(!s._2._1.equals(s._2._2)))
                .map(s->s._2._1 + " " + s._2._2)
                .distinct();
        // call pageRank code and return it's result
        return JavaPageRank.pageRank(graphCart, 100);
    }

    /**
     * this service will check for topK if available and return the number
     * we really need to return
     *
     * @param k the top k given
     * @param items the number of items available
     * @return the biggest possible
     */
    public static int getRealTopK(int k, long items){
        return (int)(k > items ? items : k);
    }

    /**
     * This service method rounds a double to 5 decimal digits
     * @param number number to format
     * @return rounded double with 5 decimal points
     */
    public static double roundFiveDecimal(double number) {
        return (double)Math.round((number) * 100000d) / 100000d;
    }

}
