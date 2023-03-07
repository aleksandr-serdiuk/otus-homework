package homework3

import org.apache.spark.sql.functions.{broadcast, col, count, desc_nulls_first, max, mean, min, round}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

object HW3_1 extends App {

  import ReadWriteUtils._

  implicit val spark = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  def processTaxiData(taxiFactsDF: DataFrame, taxiZoneDF: DataFrame): DataFrame =
    taxiFactsDF
      .join(broadcast(taxiZoneDF), col("DOLocationID") === col("LocationID"), "left")
      .groupBy(col("Borough"))
      .agg(
        count("*").as("total_trips"),
        round(min("trip_distance"), 2).as("min_distance"),
        round(mean("trip_distance"), 2).as("mean_distance"),
        round(max("trip_distance"), 2).as("max_distance")
      )
      .orderBy(col("total_trips").desc)

  val taxiFactsDF: DataFrame = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
  val taxiZoneDF: DataFrame = readCSV("src/main/resources/data/taxi_zones.csv")

  val result: DataFrame = processTaxiData(taxiFactsDF, taxiZoneDF)

  result.show()

  //write to parquet
  result.coalesce(1).write.mode("overwrite").parquet("src/main/resources/data/yellow_taxi_result")

  val taxiRes: DataFrame = readParquet("src/main/resources/data/yellow_taxi_result")
  taxiRes.show()


}
