/**
 * Submitters information - Hadoopers team:
 * Vadim Khakham 	vadim.khakham@gmail.com	311890156
 * Michel Guralnik mikijoy@gmail.com 	306555822
 * Gilad Eini 	giladeini@gmail.com	034744920
 * Adam Maor 	maorcpa.adam@gmail.com	036930501
 */

package univ.bigdata.course.movie;

import scala.Serializable;

public class PersonHelpfulness implements Comparable<PersonHelpfulness>, Serializable {

    private String userId;
    private double score;

    public PersonHelpfulness() {
    }

    public PersonHelpfulness(String userId, double score) {
        this.userId = userId;
        this.score = score;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String productId) {
        this.userId = productId;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "User id = [" + userId + "], helpfulness [" + score + "].";
    }

    @Override
    public int compareTo(PersonHelpfulness user) {
        if (this.score == user.score){
            return this.userId.compareTo(user.userId) * -1;
        }
        else {
            return this.score > user.score ? 1 : -1;
        }
    }
}
