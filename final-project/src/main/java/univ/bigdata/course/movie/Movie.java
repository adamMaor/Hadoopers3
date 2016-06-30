/**
 * Submitters information - Hadoopers team:
 * Vadim Khakham 	vadim.khakham@gmail.com	311890156
 * Michel Guralnik mikijoy@gmail.com 	306555822
 * Gilad Eini 	giladeini@gmail.com	034744920
 * Adam Maor 	maorcpa.adam@gmail.com	036930501
 */

package univ.bigdata.course.movie;

import scala.Serializable;

public class Movie implements Comparable<Movie>, Serializable {
    private String productId;

    private double score;

    public Movie() {
    }

    public Movie(String productId, double score) {
        this.productId = productId;
        this.score = score;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "Movie{" +
                "productId='" + productId + '\'' +
                ", score=" + score +
                '}';
    }

    @Override
    public int compareTo(Movie movie) {
        if (this.score == movie.score){
            return this.productId.compareTo(movie.productId) * -1;
        }
        else {
            return this.score > movie.score ? 1 : -1;
        }
    }
}
