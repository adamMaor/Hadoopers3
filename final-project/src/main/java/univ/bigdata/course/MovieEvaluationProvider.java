/**
 * Submitters information - Hadoopers team:
 * Vadim Khakham 	vadim.khakham@gmail.com	311890156
 * Michel Guralnik mikijoy@gmail.com 	306555822
 * Gilad Eini 	giladeini@gmail.com	034744920
 * Adam Maor 	maorcpa.adam@gmail.com	036930501
 */

package univ.bigdata.course;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;
import univ.bigdata.course.movie.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.*;

import static java.lang.Math.toIntExact;
import static univ.bigdata.course.movie.SerializableComparator.serialize;
import static univ.bigdata.course.MovieQueriesProvider.getRealTopK;


/**
 * Evaluation Object for producing recommendations and Mean Average Precision Calculations.
 */
public class MovieEvaluationProvider implements Serializable {
    public static final int SUGGESTIONS_NUM = 10;

    JavaRDD<MovieReview> trainSet;
    JavaRDD<MovieReview> testSet;
    JavaSparkContext sc;
    JavaPairRDD<String, Integer> movieMapping;
    JavaPairRDD<String, Integer> userMapping;
    boolean testSeparationExists;

    MovieEvaluationProvider(String trainFile) {
        createSparkContx(trainFile);
        testSeparationExists = false;
        createMapping();
    }

    MovieEvaluationProvider(String trainFile, String testFile) {
        this(trainFile);
        testSeparationExists = true;
        JavaRDD<String> fileLines = sc.textFile(testFile);
        testSet = fileLines.map(MovieReview::new);
        createMapping();
    }

    private void createSparkContx(String trainFile) {
        SparkConf conf = new SparkConf().setAppName("hw3");
        sc = new JavaSparkContext(conf);
        JavaRDD<String> fileLines = sc.textFile(trainFile);
        trainSet = fileLines.map(MovieReview::new);
    }

    private void createMapping() {
//        JavaRDD<MovieReview> set = testSeparationExists ? trainSet.union(testSet) : trainSet;
        movieMapping = trainSet
                .map(s->s.getMovie().getProductId())
                .distinct()
                .mapToPair(s->new Tuple2<>(s, s))
                .sortByKey()
                .zipWithIndex()
                .mapToPair(s->new Tuple2<>(s._1._1, toIntExact(s._2)))
                .cache();
        userMapping = trainSet
                .map(MovieReview::getUserId)
                .distinct()
                .mapToPair(s->new Tuple2<>(s, s))
                .sortByKey()
                .zipWithIndex()
                .mapToPair(s->new Tuple2<>(s._1._1, toIntExact(s._2)))
                .cache();
    }

    private MatrixFactorizationModel train(JavaRDD<MovieReview> movieReviews) {
        JavaRDD<Rating> rating = movieReviews
                // (movieStrId, (movieStrId, userStrId, score))
                .mapToPair(s -> new Tuple2<>(s.getMovie().getProductId(), new Tuple3<>(s.getMovie().getProductId(), s.getUserId(), s.getMovie().getScore())))
                // (movieStrId, ((movieStrId, userStrId, score), movieIntId))
                .join(movieMapping)
                // (userStrId, (movieIntId, userStrId, score))
                .mapToPair(s -> new Tuple2<>(s._2._1._2(), new Tuple3<>(s._2._2, s._2._1._2(), s._2._1._3())))
                // (userStrId, ((movieIntId, userStrId, score), userIntId))
                .join(userMapping)
                // (Rating)
                .map(s -> new Rating(s._2._2, s._2._1._1(), s._2._1._3()));
        return ALS.train(JavaRDD.toRDD(rating), 10, 10, 0.01);
    }

    public List<UserRecommendations> getRecommendations(List<String> users) {
        MatrixFactorizationModel model = train(trainSet);
        List<Tuple2<String, Integer>> requestedPredictions = userMapping
                .filter(s -> users.contains(s._1))
                .collect();
        List<UserRecommendations> recommendations = new LinkedList<>();
        for(Tuple2<String, Integer> user : requestedPredictions) {
            JavaPairRDD<Integer, Integer> relevantMoviesToRecommend = getRelevantMoviesForRecommend(user, trainSet);
//            System.out.print("user: " + user._1 + " " + getRelevantMoviesForRecommend(user, trainSet).collect() + "\n");
            // get RDD of movie and it's recommendations score for a specific user
            JavaPairRDD<Double, String> ratingForUser = model
                    .predict(relevantMoviesToRecommend)
                    .mapToPair(s -> new Tuple2<>(s.product(), s.rating()))
                    // (productIdInt, (rating, productIdStr))
                    .join(movieMapping.mapToPair(s -> new Tuple2<>(s._2, s._1)))
                    .mapToPair(s -> new Tuple2<>(s._2._1, s._2._2));
            List<Tuple2<Double, String>> userRecommendations = new LinkedList<>();
            ratingForUser
                    .takeOrdered(getRealTopK(SUGGESTIONS_NUM, ratingForUser.count()), serialize((o1, o2) -> o1._1.compareTo(o2._1)*-1))
                    .forEach(userRecommendations::add);
            recommendations.add(new UserRecommendations(user._1, userRecommendations));
        }
        return recommendations;
    }

    /** function to get the (user, movie) pairs for all movies a user hasn't seen yet */
    private JavaPairRDD<Integer, Integer> getRelevantMoviesForRecommend(Tuple2<String, Integer> user, JavaRDD<MovieReview> set) {
        JavaPairRDD<String, Boolean> moviesCurrentUserSaw = set
                // RDD of (movie, current user saw movie) pairs
                .mapToPair(s -> new Tuple2<>(s.getMovie().getProductId(), s.getUserId().equals(user._1)))
                .reduceByKey((a, b) -> a | b);
        return set
                .mapToPair(s -> new Tuple2<>(s.getMovie().getProductId(), null))
                .distinct()
                // (movie, (null, currentUserSawBool))
                .join(moviesCurrentUserSaw)
                // keep only movies current user hasn't seen
                .filter(s->!s._2._2)
                .mapToPair(s->new Tuple2<>(s._1, null))
                .join(movieMapping)
                .mapToPair(s -> new Tuple2<>(user._2, s._2._2));
    }

    public Double map() {
        MatrixFactorizationModel model = train(trainSet);
        // All users that exists in test set indexes
        JavaRDD<Integer> testUsers = userMapping
                .join(testSet.mapToPair(s -> new Tuple2<>(s.getUserId(), null)).distinct())
                .map(s -> s._2._1);
        List<Integer> testUsersArr = testUsers.collect();
        double counter = 0, sum = 0;
        for (Integer user : testUsersArr) {
            List<Rating> predictions = Arrays.asList(model.recommendProducts(user, 50000));
            if (predictions.size() != 0) {
                sum += mapValueForUser(predictions, user);
                counter++;
            }
        }
        System.out.println("num of users :" + testUsersArr.size());
        System.out.println("sum :" + sum + " counter: " + counter);
        return sum/(counter != 0 ? counter : 1);
    }

    double mapValueForUser(List<Rating> predictions, Integer user) {
        String userStringId = userMapping.filter(s -> s._2.equals(user)).collect().get(0)._1;
        // List of tuples (movieIntId, rank)
        List<Tuple2<Integer, Integer>> movieRankings = new ArrayList<>();
        for (int i = 0; i < predictions.size(); i++){
            movieRankings.add(new Tuple2<>(predictions.get(i).product(), i));
        }
        JavaPairRDD<Integer, Object> moviesInTestThatAlsoAppearInTrainThatUserLiked = testSet
                // keep all the movies that test users liked
                .filter(s -> s.getMovie().getScore() >= 3 && userStringId.equals(s.getUserId()))
                .mapToPair(s -> new Tuple2<>(s.getMovie().getProductId(), null))
                .join(movieMapping)
                .mapToPair(s -> new Tuple2<>(s._2._2, null));
        long maxHitRecommendationsForUser = moviesInTestThatAlsoAppearInTrainThatUserLiked.count();
        List<Integer> sortedRankingListOfHits = sc
                .parallelizePairs(movieRankings)
                .join(moviesInTestThatAlsoAppearInTrainThatUserLiked)
                // javaRDD of rankings
                .mapToPair(s -> new Tuple2<>(s._2._1, null))
                .sortByKey()
                .map(s -> s._1)
                .collect();
        double map = 0;
        System.out.println("\n\n" + movieRankings + "\n");
        System.out.println(sortedRankingListOfHits);
        for (int i = 0; i < sortedRankingListOfHits.size(); i++) {
            map += (i+1)/(double)(sortedRankingListOfHits.get(i)+1);
            System.out.println("map: " + map + " rank: " + (i+1) + " divide: " + (sortedRankingListOfHits.get(i)+1));
        }
        return map / maxHitRecommendationsForUser;
    }
}