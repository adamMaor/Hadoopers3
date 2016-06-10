package univ.bigdata.course.movie;

import scala.Serializable;

import java.util.Comparator;

public interface SerializableComparator<T> extends Comparator<T>, Serializable {

    static <T> SerializableComparator<T> serialize(SerializableComparator<T> comparator) {
        return comparator;
    }

}