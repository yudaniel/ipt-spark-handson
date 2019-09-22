package ch.ipt.handson;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class SimpleLocalExample {

    public static void main(String[]args) {
        // Create a new local SparkSession
        SparkSession spark =
                SparkSession.builder()
                        .master("local")
                        .getOrCreate();

        // Set the loglevel to ERROR
        SparkContext context = SparkContext.getOrCreate();
        context.setLogLevel("ERROR");

        // count on spark
        long count = spark.range(1,2000).count();
        System.out.println(count);
    }
}
