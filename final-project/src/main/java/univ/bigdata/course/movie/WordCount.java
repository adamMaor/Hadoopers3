package univ.bigdata.course.movie;

import scala.Serializable;

public class WordCount implements Comparable<WordCount>, Serializable {
    public String word;
    public Integer count;

    public WordCount(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public int compareTo(WordCount o) {
        if (this.count.equals(o.count)){
            return this.word.compareTo(o.word) * -1;
        }
        else {
            return this.count.compareTo(o.count);
        }
    }
    @Override
    public String toString(){
        return "Word = [" + word + "], number of occurrences [" + count + "].";
    }
}
