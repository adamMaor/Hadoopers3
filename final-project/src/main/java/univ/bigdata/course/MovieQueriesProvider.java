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
import scala.Tuple3;
import univ.bigdata.course.movie.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import static java.lang.Math.toIntExact;
import static univ.bigdata.course.movie.SerializableComparator.serialize;

import java.util.*;


public class MovieQueriesProvider implements Serializable{
    public static final int SUGGESTIONS_NUM = 10;
    JavaRDD<MovieReview> movieReviews;
    /**
     * Constructor method
     */
    MovieQueriesProvider(String inputFile) {
        SparkConf conf = new SparkConf().setAppName("hw3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fileLines = sc.textFile(inputFile);
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
        if (filteredMovieReviews.isEmpty()){
            return 0;
        }
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
                .top(getRealTopK(topK, moviesCount()));
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
        // we will "trick" movie to have the count instead of the score in order to use it's comparator
        return movieReviews
                .mapToPair(s -> new Tuple2<>(s.getMovie().getProductId(), 1))
                .reduceByKey((a, b) -> a + b)
                .map(s -> new Movie(s._1(), s._2()))
                .top(getRealTopK(topK, moviesCount()));
    }

    /**
     * Computes most popular movie which has been reviewed by at least
     * numOfUsers (provided as parameter).
     *
     * @param numOfUsers - limit of minimum users which reviewed the movie
     * @return - movie which got highest count of reviews
     */
    Movie mostPopularMovieReviewedByKUsers(final int numOfUsers) {
        List<Movie> popularMovieFiltered =  movieReviews
            .mapToPair(s-> new Tuple2<>(s.getMovie().getProductId(), new Tuple2<>(s.getMovie().getScore(), 1)))
            .reduceByKey((a, b)-> new Tuple2<>(a._1 + b._1, a._2 + b._2))
            .filter(s -> s._2._2 >= numOfUsers)
            .map(s -> new Movie(s._1, roundFiveDecimal(s._2._1 / s._2._2)))
            .top(1);
        return popularMovieFiltered.isEmpty()? null : popularMovieFiltered.get(0);
    }

    /**
     * Compute map of words count for top K words
     *
     * @param topK - top k number
     */
    List<WordCount> moviesReviewWordsCount(final int topK) {
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
    List<Person> topKHelpfullUsers(final int k) {
        JavaRDD<Person> helpfulUsers = movieReviews
            .mapToPair(a -> new Tuple2<>(a.getUserId(), a.getHelpfulness()))
            .mapToPair(s -> new Tuple2<>(s._1, new Tuple2<>(Double.parseDouble(s._2.substring(0,s._2.lastIndexOf('/'))),
                    Double.parseDouble(s._2.substring(s._2.lastIndexOf('/') + 1, s._2.length())))))
            .filter(s -> s._2._2 != 0)
            .reduceByKey((a,b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2))
            .map(s -> new Person(s._1, roundFiveDecimal(s._2._1/s._2._2)));
        return helpfulUsers.top(getRealTopK(k,helpfulUsers.count() ));
    }

    /**
     * Total movies count
     */
    long moviesCount() {
        JavaRDD<String> distinctMovies = movieReviews.map(s->s.getMovie().getProductId()).distinct();
        return distinctMovies.count();
    }

    List<Person> getPageRank() throws Exception {
        JavaPairRDD<String, String> graph = movieReviews
                .mapToPair(s->new Tuple2<>(s.getMovie().getProductId(), s.getUserId()));
        JavaRDD<String> graphCart =
                graph.cartesian(graph)
                .filter(s->(s._1._1.equals(s._2._1) && !s._1._2.equals(s._2._2)))
                .map(s->s._1._2 + " " + s._2._2)
                .distinct();
        return JavaPageRank.pageRank(graphCart, 100);
    }

    List<UserRecommendations> getRecommendations(List<String> users) {
        JavaPairRDD<String, Integer> movies = movieReviews
                .map(s->s.getMovie().getProductId())
                .distinct()
                .zipWithIndex()
                .mapToPair(s->new Tuple2<>(s._1, toIntExact(s._2)));;
        JavaPairRDD<String, Integer> reviewers = movieReviews
                .map(MovieReview::getUserId)
                .distinct()
                .zipWithIndex()
                .mapToPair(s->new Tuple2<>(s._1, toIntExact(s._2)));
        JavaRDD<Rating> rating = movieReviews
                // (movieStrId, (movieStrId, userStrId, score))
                .mapToPair(s->new Tuple2<>(s.getMovie().getProductId(), new Tuple3<>(s.getMovie().getProductId(), s.getUserId(), s.getMovie().getScore())))
                // (movieStrId, ((movieStrId, userStrId, score), movieIntId))
                .join(movies)
                // (userStrId, (movieIntId, userStrId, score))
                .mapToPair(s->new Tuple2<>(s._2._1._2(), new Tuple3<>(s._2._2, s._2._1._2(), s._2._1._3())))
                // (userStrId, ((movieIntId, userStrId, score), userIntId))
                .join(reviewers)
                // (Rating)
                .map(s->new Rating(s._2._2, s._2._1._1(), s._2._1._3()));
        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(rating), 10, 10, 0.01);
        List<Tuple2<String, Integer>> requestedPredictions = reviewers
                .filter(s -> users.contains(s._1))
                .collect();
        List<UserRecommendations> recommendations = new LinkedList<>();
        for(Tuple2<String, Integer> user : requestedPredictions) {
            JavaPairRDD<Double, String> ratingForUser = rating
                    .wrapRDD(model.predict(JavaRDD.toRDD(movies.map(s -> new Tuple2<>(user._2, s._2)))))
                    .mapToPair(s -> new Tuple2<>(s.product(), s.rating()))
                    // (productIdInt, (rating, productIdStr))
                    .join(movies.mapToPair(s -> new Tuple2<>(s._2, s._1)))
                    .mapToPair(s -> new Tuple2<>(s._2._1, s._2._2));
            List<Tuple2<Double, String>> userRecommendations = new LinkedList<>();
            ratingForUser
                    .takeOrdered(getRealTopK(SUGGESTIONS_NUM, ratingForUser.count()), serialize((o1, o2) -> o1._1.compareTo(o2._1)*-1))
                    .forEach(userRecommendations::add);
            recommendations.add(new UserRecommendations(user._1, userRecommendations));
        }
        return recommendations;
    }

    /**
     * this service will check for topK if available and return the number
     * we really need to return
     *
     * @param k the top k given
     * @return the biggest possible
     */
    int getRealTopK(int k, long items){
        return (int)(k > items ? items : k);
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
