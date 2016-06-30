/**
 * Submitters information - Hadoopers team:
 * Vadim Khakham 	vadim.khakham@gmail.com	311890156
 * Michel Guralnik mikijoy@gmail.com 	306555822
 * Gilad Eini 	giladeini@gmail.com	034744920
 * Adam Maor 	maorcpa.adam@gmail.com	036930501
 */

package univ.bigdata.course.movie;

import scala.Serializable;

/**
 *  this class is the same as Movie but keeps a review count instead of score.
 *  we made this for code readability
 */
public class MovieCountedReview implements Comparable<MovieCountedReview>, Serializable {
    private String productId;

    private int reviewCount;

    public MovieCountedReview() {
    }

    public MovieCountedReview(String productId, int reviewCount) {
        this.productId = productId;
        this.reviewCount = reviewCount;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public int getScore() {
        return reviewCount;
    }

    public void setScore(int score) {
        this.reviewCount = score;
    }

    @Override
    public String toString() {
        return "Movie product id = [" + productId + "], reviews count [" + reviewCount + "].";
    }

    @Override
    public int compareTo(MovieCountedReview movie) {
        if (this.reviewCount == movie.reviewCount){
            return this.productId.compareTo(movie.productId) * -1;
        }
        else {
            return this.reviewCount > movie.reviewCount ? 1 : -1;
        }
    }



}
