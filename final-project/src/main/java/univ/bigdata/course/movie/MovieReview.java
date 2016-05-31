/**
 * Submitters information - Hadoopers team:
 * Vadim Khakham 	vadim.khakham@gmail.com	311890156
 * Michel Guralnik mikijoy@gmail.com 	306555822
 * Gilad Eini 	giladeini@gmail.com	034744920
 * Adam Maor 	maorcpa.adam@gmail.com	036930501
 */
package univ.bigdata.course.movie;

import scala.Serializable;

import java.util.Date;

public class MovieReview implements Serializable {

    private Movie movie;

    private String userId;

    private String profileName;

    private String helpfulness;

    private Date timestamp;

    private String summary;

    private String review;

    public MovieReview() {
    }

    public MovieReview(String review) {
        String[] reviewParamsList = review.split("\\t");
        for (int i = 0; i < reviewParamsList.length; i++){
            reviewParamsList[i] = reviewParamsList[i].substring(reviewParamsList[i].indexOf(":") + 2);
        }
        Movie movie = new Movie(reviewParamsList[0], Double.parseDouble(reviewParamsList[4]));
        Date movieDate = new Date(Long.parseLong(reviewParamsList[5]));
        this.movie = movie;
        this.userId = reviewParamsList[1];
        this.profileName = reviewParamsList[2];
        this.helpfulness =reviewParamsList[3];
        this.timestamp = movieDate;
        this.summary = reviewParamsList[6];
        this.review = reviewParamsList[7];
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getProfileName() {
        return profileName;
    }

    public void setProfileName(String profileName) {
        this.profileName = profileName;
    }

    public String getHelpfulness() {
        return helpfulness;
    }

    public void setHelpfulness(String helpfulness) {
        this.helpfulness = helpfulness;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getReview() {
        return review;
    }

    public void setReview(String review) {
        this.review = review;
    }

    public Movie getMovie() {
        return movie;
    }

    public void setMovie(Movie movie) {
        this.movie = movie;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    @Override
    public String toString() {
        return "MovieReview{" +
                "movie=" + movie +
                ", userId='" + userId + '\'' +
                ", profileName='" + profileName + '\'' +
                ", helpfulness='" + helpfulness + '\'' +
                ", timestamp=" + timestamp +
                ", summary='" + summary + '\'' +
                ", review='" + review + '\'' +
                '}';
    }
}
