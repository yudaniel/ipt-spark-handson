package ch.ipt.handson;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Use the google storage connector https://cloud.google.com/dataproc/docs/tutorials/gcs-connector-spark-tutorial
 */
public class RDDWordCountExample {
    public static void main(String[] args){
        // Read the arguments
        if (args.length != 2) {
            throw new IllegalArgumentException("Exactly 2 arguments are required: <inputUri> <outputUri>");
        }
        String inputPath = args[0];
        String outputPath = args[1];

        // Create a Spark context
        new SparkConf().setAppName("Word Count").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext();

        // Read the text
        JavaRDD<String> lines = sparkContext.textFile(inputPath);

        // Flatmap the word in the text and merge the tuples
        JavaRDD<String> words = lines.flatMap(
                (String line) -> Arrays.asList(line.split(" ")).iterator()
        );
        JavaPairRDD<String, Integer> wordCounts = words.mapToPair(
                (String word) -> new Tuple2<>(word, 1)
        ).reduceByKey(
                (Integer count1, Integer count2) -> count1 + count2
        );

        // save the result
        wordCounts.saveAsTextFile(outputPath);

        // peak the top
        List<Tuple2<String, Integer>> list = wordCounts.top(10);
        for (Tuple2<String, Integer> t : list){
            System.out.println(t._1() +  ":" + t._2());
        }
    }

}
