package univ.bigdata.course.movie;

import scala.Serializable;
import univ.bigdata.course.MovieQueriesProvider;

/**
 * Person class from page rank.
 */
public class Person implements Comparable<Person>, Serializable {
    public String id;
    public double score;

    public Person(String id, double score) {
        this.id = id;
        this.score = score;
    }

    @Override
    public String toString() {
        return "Person{" +
                "UserId='" + id + '\'' +
                ", PageRank=" + MovieQueriesProvider.roundFiveDecimal(score) +
                '}';
    }

    @Override
    public int compareTo(Person person) {
        if (this.score == person.score){
            return this.id.compareTo(person.id) * -1;
        }
        else {
            return this.score > person.score ? 1 : -1;
        }
    }

}
