package univ.bigdata.course.movie;

import scala.Serializable;
import scala.Tuple2;

import java.util.List;

public class UserRecommendations implements Serializable {
    public String userId;
    public List<Tuple2<Double, String>> movieRecommendations;

    public UserRecommendations(String userId, List<Tuple2<Double, String>> MovieRecommendations) {
        this.userId = userId;
        this.movieRecommendations = MovieRecommendations;
    }

    @Override
    public String toString(){
        String retStr =  "Recommendations for " + userId + ":\n";
        for(int i = 1; i <= movieRecommendations.size(); i++) {
           retStr += i + ". " + movieRecommendations.get(i-1)._2 + "   |   Score:" + movieRecommendations.get(i-1)._1 + "\n";
        }
        return retStr;
    }
}
