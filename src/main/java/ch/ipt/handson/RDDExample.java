package ch.ipt.handson;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class RDDExample {

    public static void main(String[] args){
        // Create a new local SparkSession
        SparkSession spark =
                SparkSession.builder()
                        .master("local")
                        .getOrCreate();

        // Set the loglevel to ERROR
        SparkContext sc = SparkContext.getOrCreate();
        sc.setLogLevel("ERROR");

        // RDD example
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        JavaRDD<Integer> intRDD = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 50, 61, 72, 8, 9, 19, 31, 42, 53, 6, 7, 23));
        JavaRDD<Integer> filteredRDD = intRDD.filter((x) -> (x > 10 ? false : true));
        JavaRDD<Integer> transformedRDD = filteredRDD.map((x) -> (x * x) );
        int sumTransformed = transformedRDD.reduce((x, y) -> (x + y));

        System.out.println(sumTransformed);

    }
}
