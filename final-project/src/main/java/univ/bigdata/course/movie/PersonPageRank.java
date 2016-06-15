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
