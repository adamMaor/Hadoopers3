/**
 * Submitters information - Hadoopers team:
 * Vadim Khakham 	vadim.khakham@gmail.com	311890156
 * Michel Guralnik mikijoy@gmail.com 	306555822
 * Gilad Eini 	giladeini@gmail.com	034744920
 * Adam Maor 	maorcpa.adam@gmail.com	036930501
 */

package univ.bigdata.course;

import org.apache.commons.beanutils.converters.IntegerArrayConverter;
import org.apache.spark.Accumulator;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static java.lang.Math.toIntExact;
import static univ.bigdata.course.movie.SerializableComparator.serialize;
import static univ.bigdata.course.MovieQueriesProvider.getRealTopK;

public class MovieEvaluationProvider implements Serializable{
    JavaRDD<MovieReview> trainSet;
    JavaRDD<MovieReview> testSet;
    Tuple2<Double, Integer> meanAveragePrecision;
    public static final int SUGGESTIONS_NUM = 10;
    JavaSparkContext sc;

    MovieEvaluationProvider(String trainFile) {
        SparkConf conf = new SparkConf().setAppName("hw3");
        sc = new JavaSparkContext(conf);
        JavaRDD<String> fileLines = sc.textFile(trainFile);
        trainSet = fileLines.map(MovieReview::new);
    }

    MovieEvaluationProvider(String trainFile, String testFile) {
        SparkConf conf = new SparkConf().setAppName("hw3");
        sc = new JavaSparkContext(conf);
        JavaRDD<String> fileLines = sc.textFile(trainFile);
        trainSet = fileLines.map(MovieReview::new);
        fileLines = sc.textFile(trainFile);
        testSet = fileLines.map(MovieReview::new).filter(s -> s.getMovie().getScore() >= 3);
        meanAveragePrecision = new Tuple2<>(0D,0);
    }

    List<UserRecommendations> getRecommendations(List<String> users) {
        JavaPairRDD<String, Integer> movies = trainSet
                .map(s->s.getMovie().getProductId())
                .distinct()
                .zipWithIndex()
                .mapToPair(s->new Tuple2<>(s._1, toIntExact(s._2)));;
        JavaPairRDD<String, Integer> reviewers = trainSet
                .map(MovieReview::getUserId)
                .distinct()
                .zipWithIndex()
                .mapToPair(s->new Tuple2<>(s._1, toIntExact(s._2)));
        JavaRDD<Rating> rating = trainSet
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

    Double map() {
        /*JavaRDD<String> users = testSet
                .map(s -> s.getUserId())
                .distinct();
        JavaPairRDD<String, Tuple2<String, Integer>> recommendations = getRecommendations();
        final Accumulator<Double> userMap = sc.accumulator(0D);
        users.foreach(s -> {
            JavaRDD<Integer> testMoviesRank = recommendations
                    .filter(a -> a._1 == s && !trainSet.filter(b -> b.getMovie().getProductId() == a._2._1).isEmpty())
                    .map(a -> a._2._2);
            testMoviesRank.foreach(a -> {
                userMap.add((double)(testMoviesRank.filter(b -> b <= a).count()/a));
            });
            userMap.setValue(0D);
        });*/

        return 0.0;
    }
}

