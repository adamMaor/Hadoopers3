/**
 * Submitters information - Hadoopers team:
 * Vadim Khakham 	vadim.khakham@gmail.com	311890156
 * Michel Guralnik mikijoy@gmail.com 	306555822
 * Gilad Eini 	giladeini@gmail.com	034744920
 * Adam Maor 	maorcpa.adam@gmail.com	036930501
 */

package univ.bigdata.course;

import org.apache.spark.api.java.JavaPairRDD;
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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

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
        testSet = fileLines.map(MovieReview::new).filter(s -> s.getMovie().getScore() >= 3);
        createMapping();
    }

    private void createSparkContx(String trainFile) {
        SparkConf conf = new SparkConf().setAppName("hw3");
        sc = new JavaSparkContext(conf);
        JavaRDD<String> fileLines = sc.textFile(trainFile);
        trainSet = fileLines.map(MovieReview::new);
    }

    void createMapping() {
        JavaRDD<MovieReview> set = testSeparationExists ? trainSet.union(testSet) : trainSet;
        movieMapping = set
                .map(s->s.getMovie().getProductId())
                .distinct()
                .zipWithIndex()
                .mapToPair(s->new Tuple2<>(s._1, toIntExact(s._2)));
        userMapping = set
                .map(MovieReview::getUserId)
                .distinct()
                .zipWithIndex()
                .mapToPair(s->new Tuple2<>(s._1, toIntExact(s._2)));
    }

    MatrixFactorizationModel train(JavaRDD<MovieReview> movieReviews) {
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

    List<UserRecommendations> getRecommendations(List<String> users) {
        MatrixFactorizationModel model = train(trainSet);
        List<Tuple2<String, Integer>> requestedPredictions = userMapping
                .filter(s -> users.contains(s._1))
                .collect();
        List<UserRecommendations> recommendations = new LinkedList<>();
        for(Tuple2<String, Integer> user : requestedPredictions) {
            JavaPairRDD<Integer, Integer> relevantMoviesToRecommend = getRelevantMoviesForRecommend(user);
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

    private JavaPairRDD<Integer, Integer> getRelevantMoviesForRecommend(Tuple2<String, Integer> user) {
        return trainSet
                .filter(s -> !s.getUserId().equals(user._1))
                .mapToPair(s -> new Tuple2<>(s.getMovie().getProductId(), null))
                .distinct()
                .join(movieMapping)
                .mapToPair(s -> new Tuple2<>(user._2, s._2._2));
    }

    Double map() {
        MatrixFactorizationModel model = train(trainSet);
        JavaPairRDD<Integer, Integer> moviesForTestUsers = testSet
                .mapToPair(s -> new Tuple2<>(s.getUserId(), null))
                .distinct()
                .join(userMapping)
                .map(s -> s._2._2)
                .cartesian(movieMapping.map(s -> s._2));
        System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n###" + moviesForTestUsers.count());
        

        return 0.0;
    }
}


//    JavaPairRDD<Object, List<Rating>> trainSetRecommendations = model
//                .recommendProductsForUsers(Integer.MAX_VALUE).toJavaRDD()
//                .mapToPair(s->new Tuple2<>(s._1, Arrays.asList(s._2)));

