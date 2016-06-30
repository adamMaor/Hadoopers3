/**
 * Submitters information - Hadoopers team:
 * Vadim Khakham 	vadim.khakham@gmail.com	311890156
 * Michel Guralnik mikijoy@gmail.com 	306555822
 * Gilad Eini 	giladeini@gmail.com	034744920
 * Adam Maor 	maorcpa.adam@gmail.com	036930501
 */

package univ.bigdata.course.movie;

import scala.Serializable;
import scala.Tuple2;

import java.util.List;

/**
 * a Class to print out User Recommendations
 */
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
           retStr += i + ". " + movieRecommendations.get(i-1)._2 + "\n";
        }
        return retStr;
    }
}
