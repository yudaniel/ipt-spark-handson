package ch.ipt.handson;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static javafx.beans.binding.Bindings.select;
import static org.apache.spark.sql.functions.*;

public class FilterDataExample {
    public static void main(String[] args) {
        SparkSession spark =
                SparkSession.builder()
                        .master("local")
                        .getOrCreate();

        SparkContext sc = SparkContext.getOrCreate();
        sc.setLogLevel("ERROR");

        Dataset<Row> singleDayRetailRaw = spark.read().format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(ProgramConstants.SINGLE_DAY_RETAIL_PATH);

        singleDayRetailRaw.show();

        Dataset<Row> five = singleDayRetailRaw.select(lit(5), lit("five"), lit(5.0));
        five.show(5);

        System.out.println("File length: " + singleDayRetailRaw.count());

        System.out.println("Five length: " + five.count());

        singleDayRetailRaw.where(col("InvoiceNo").notEqual(536365))
                .select("InvoiceNo", "Description")
                .show(5, false);

        Column priceFilter = col("UnitPrice").$greater(500);
        Column postageFilter = col("Description").contains("POSTAGE");
        singleDayRetailRaw.where(singleDayRetailRaw.col("StockCode").isin("DOT"))
                .where(priceFilter.or(postageFilter)).show();

        singleDayRetailRaw.where(col("UnitPrice").gt(10)).sort(desc("UnitPrice")).show(30);

        double averagePrice = singleDayRetailRaw.select(avg(col("UnitPrice"))).head().getDouble(0);
        Dataset<Row> itemsAboveAverage = singleDayRetailRaw.withColumn("aboveAvgPrice", col("UnitPrice").geq(5 * averagePrice))
                .filter("aboveAvgPrice")
                .sort(asc("UnitPrice"));

        itemsAboveAverage.select("Description", "UnitPrice", "aboveAvgPrice").show(false);

        singleDayRetailRaw.where(col("Description").eqNullSafe("hello")).show();

        List<String> colors = Arrays.asList("black", "white", "red", "green", "blue", "yellow");
        colors.replaceAll(String::toUpperCase);
        Dataset<Row> coloredItems = singleDayRetailRaw.filter(
                regexp_extract(
                        col("Description"),
                        colors.stream().collect(Collectors.joining("|", "(", ")")), 1
                ).alias("color_clean").notEqual(""));

        coloredItems.show(false);
    }
}
