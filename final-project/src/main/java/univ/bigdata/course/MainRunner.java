package univ.bigdata.course;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;


public class MainRunner {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("hw3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> distFile = sc.textFile("C:\\GitHub\\Hadoopers3\\movies-simple.txt");
        System.out.println(distFile.count());
    }
}
