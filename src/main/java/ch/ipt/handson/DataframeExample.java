package ch.ipt.handson;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class DataframeExample {
    public static void main(String[] args) {
        final int DEFAULT_NUM_ROWS = 5;

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

        // read all the data
        Dataset<Row> ds = spark.read().format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(path);


        ds.printSchema();
        ds.show(2 * DEFAULT_NUM_ROWS);

        // items per invoice
        ds.groupBy("InvoiceNo").count().sort(desc("count")).show();

        // convert string column to timestamp column
        Column invoiceDateColumn = to_timestamp(col("invoiceDate"), "MM/dd/yyyy HH:mm");
        ds = ds.withColumn("invoiceDate", invoiceDateColumn);

        Dataset<Row> purchasePerCustomerPerDay = ds.selectExpr("CustomerID",
                "(UnitPrice * Quantity) as total_cost",
                "InvoiceDate")
                .groupBy(col("CustomerID"), window(col("InvoiceDate"), "1 day"))
                .sum("total_cost")
                .sort(desc("sum(total_cost)"))
                .withColumn("sum(total_cost)", round(col("sum(total_cost)"), 2));
//                .withColumnRenamed("sum(total_cost)", "spent")
//                .withColumnRenamed("window", "timeframe");

        purchasePerCustomerPerDay.explain();

        purchasePerCustomerPerDay = purchasePerCustomerPerDay.na().drop();
        purchasePerCustomerPerDay.show(DEFAULT_NUM_ROWS, false);

        Dataset<Row> cleanedDataset = ds
                .na().fill(0L)
                .withColumn("DayOfWeek", date_format(col("InvoiceDate"), "EEEE"))
                .repartition(4);
        System.out.println("Number of partitions: " + cleanedDataset.rdd().partitions().length);
        cleanedDataset.show(DEFAULT_NUM_ROWS);

        // These could also be discarded - depends on whether we think this is valuable
        cleanedDataset.where("CustomerID == 0").show(DEFAULT_NUM_ROWS);
    }
}
