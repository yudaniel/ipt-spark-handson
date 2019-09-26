package ch.ipt.handson;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

/*
* More Information: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
* */
public class StreamingDataframeExample {
    public static void main(String[] args) throws StreamingQueryException, InterruptedException {

        // Create a new local SparkSession
        SparkSession spark =
                SparkSession.builder()
                        .master("local")
                        .getOrCreate();

        SparkContext sc = SparkContext.getOrCreate();
        sc.setLogLevel("ERROR");

        // InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
        StructType retailSchema = new StructType()
                .add("InvoiceNo", "string")
                .add("StockCode", "string")
                .add("Description", "string")
                .add("Quantity", "integer")
                .add("InvoiceDate", "timestamp")
                .add("UnitPrice", "double")
                .add("CustomerID", "double")
                .add("Country", "string");

        Dataset<Row> streamingDataframe = spark.readStream().format("csv")
                .schema(retailSchema)
                .option("sep", ",")
                .option("header", "true")
                .option("maxFilesPerTrigger", 1)
                // .load(ProgramConstants.DAILY_RETAIL_PATH);
                .load(ProgramConstants.SUBSET_DAILY_RETAIL_PATH);

        streamingDataframe.printSchema();

        Dataset<Row> purchasePerCustomerPerHour = streamingDataframe.na().drop()
                .selectExpr(
                "cast(CustomerID as int) CustomerID",
                "(UnitPrice * Quantity) as total_cost",
                "InvoiceDate")
                .groupBy(col("CustomerID"), window(col("InvoiceDate"), "1 hour"))
                .sum("total_cost")
                .withColumnRenamed("sum(total_cost)", "spent");

        /*
        // This will output everything in the console
        StreamingQuery purchasePerCustomerPerMonthConsoleQuery = purchasePerCustomerPerHour.writeStream()
                .format("console")
                .outputMode("complete")
                .start();
        purchasePerCustomerPerMonthConsoleQuery.awaitTermination();
        */

        StreamingQuery createPurchasePerCustomerPerMonthTable = purchasePerCustomerPerHour.writeStream()
                .format("memory")
                .queryName("customer_purchases")
                .outputMode("complete")
                .start();

        createPurchasePerCustomerPerMonthTable.awaitTermination(10000);

        // look for CustomerID in the logs to find the output
        for (int i = 0; i < 10; i++) {
            spark.sql("SELECT * " +
                "FROM customer_purchases " +
                "ORDER BY spent DESC")
                .show(10, false);
            Thread.sleep(1000);
        }

        createPurchasePerCustomerPerMonthTable.stop();

    }
}
