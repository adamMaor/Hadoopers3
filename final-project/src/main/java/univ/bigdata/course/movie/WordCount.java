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
 * a Class to compare and print Word Counts
 */
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
