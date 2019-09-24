package ch.ipt.handson;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class SparkAggregations {
    public static void main(String[] args) {

        // Create a new local SparkSession
        SparkSession spark =
                SparkSession.builder()
                        .master("local")
                        .getOrCreate();

        // Set the loglevel to ERROR
        SparkContext sc = SparkContext.getOrCreate();
        sc.setLogLevel("ERROR");

        // Read the online-retail-dataset.csv
        Dataset<Row> dailySalesDataset = spark.read().format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(ProgramConstants.DAILY_RETAIL_PATH)
                .withColumn("CustomerID", col("CustomerID").cast("integer"));

        dailySalesDataset.printSchema();

        // what do these queries do?
        dailySalesDataset.select(count("StockCode")).show();
        dailySalesDataset.select(countDistinct("StockCode")).show();
        dailySalesDataset.select(approx_count_distinct("StockCode", 0.1)).show();
        dailySalesDataset.select(first("StockCode"), last("StockCode")).show();
        dailySalesDataset.select(min("Quantity"), max("Quantity")).show();
        dailySalesDataset.select(sum("Quantity")).show();
        dailySalesDataset.select(sumDistinct("Quantity")).show();

        // what's the difference between select and selectExpr?
        dailySalesDataset.select(
                count("Quantity").alias("total_transactions"),
                sum("Quantity").alias("total_purchases"),
                avg("Quantity").alias("avg_purchases"),
                expr("mean(Quantity)").alias("mean_purchases"))
                .selectExpr(
                        "total_purchases/total_transactions",
                        "avg_purchases",
                        "mean_purchases")
                .show();

        dailySalesDataset.select(var_pop("Quantity"),
                var_samp("Quantity"),
                stddev_pop("Quantity"),
                stddev_samp("Quantity"))
                .show();

        dailySalesDataset.select(
                corr("InvoiceNo", "Quantity"),
                covar_samp("InvoiceNo", "Quantity"),
                covar_pop("InvoiceNo", "Quantity"))
                .show();

        dailySalesDataset.agg(
                collect_set("Country"),
                collect_list("Country"))
                .show();

        dailySalesDataset.groupBy("InvoiceNo")
                .agg(
                        count("Quantity").alias("quan"),
                        expr("count(Quantity)"))
                .show();

        dailySalesDataset.groupBy("InvoiceNo").
                agg(expr("avg(Quantity)"), expr("stddev_pop(Quantity)"))
                .show();
    }
}
