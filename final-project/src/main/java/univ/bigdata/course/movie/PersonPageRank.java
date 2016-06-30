/**
 * Submitters information - Hadoopers team:
 * Vadim Khakham 	vadim.khakham@gmail.com	311890156
 * Michel Guralnik mikijoy@gmail.com 	306555822
 * Gilad Eini 	giladeini@gmail.com	034744920
 * Adam Maor 	maorcpa.adam@gmail.com	036930501
 */

package univ.bigdata.course.movie;

import scala.Serializable;
import univ.bigdata.course.MovieQueriesProvider;

/**
 * PersonPageRank class from page rank.
 */
public class PersonPageRank implements Comparable<PersonPageRank>, Serializable {
    public String id;
    public double score;

    public PersonPageRank(String id, double score) {
        this.id = id;
        this.score = score;
    }

    @Override
    public String toString() {
        return "PersonPageRank{" +
                "UserId='" + id + '\'' +
                ", PageRank=" + MovieQueriesProvider.roundFiveDecimal(score) +
                '}';
    }

    @Override
    public int compareTo(PersonPageRank personPageRank) {
        if (this.score == personPageRank.score){
            return this.id.compareTo(personPageRank.id) * -1;
        }
        else {
            return this.score > personPageRank.score ? 1 : -1;
        }
    }

}
