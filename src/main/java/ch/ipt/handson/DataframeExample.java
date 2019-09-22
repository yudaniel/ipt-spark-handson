package ch.ipt.handson;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.net.URL;

import static org.apache.spark.sql.functions.desc;

public class DataframeExample {
    public static void main(String[] args){
        // Create a new local SparkSession
        SparkSession spark =
                SparkSession.builder()
                        .master("local")
                        .getOrCreate();

        // Set the loglevel to ERROR
        SparkContext sc = SparkContext.getOrCreate();
        sc.setLogLevel("ERROR");

        // Read the online-retail-dataset.csv
        String path = "src/main/resources/online-retail-dataset.csv";
        Dataset<Row> ds = spark.read().format("csv").option("header", "true").load(path);

        ds.groupBy("InvoiceNo").count().sort(desc("count")).show();
    }
}
