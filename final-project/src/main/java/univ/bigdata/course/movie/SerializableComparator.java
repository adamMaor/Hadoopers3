/**
 * Submitters information - Hadoopers team:
 * Vadim Khakham 	vadim.khakham@gmail.com	311890156
 * Michel Guralnik mikijoy@gmail.com 	306555822
 * Gilad Eini 	giladeini@gmail.com	034744920
 * Adam Maor 	maorcpa.adam@gmail.com	036930501
 */

package univ.bigdata.course.movie;

import scala.Serializable;

import java.util.Comparator;

public interface SerializableComparator<T> extends Comparator<T>, Serializable {

    static <T> SerializableComparator<T> serialize(SerializableComparator<T> comparator) {
        return comparator;
    }

}