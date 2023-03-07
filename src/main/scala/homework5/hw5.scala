package homework5

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.{broadcast, col, count, desc_nulls_first, max, mean, min, round}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col,hour}

import java.io._

object hw5 {
  def readParquet(path: String)(implicit spark: SparkSession): DataFrame = spark.read.load(path)
  def readCSV(path: String)(implicit spark: SparkSession): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

  def writeParquet(resDF: DataFrame, numPartitions: Int, path: String)(implicit spark: SparkSession) =
    resDF.coalesce(numPartitions).write.mode("overwrite").parquet(path)

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

  case class TaxiRide(
                       VendorID: Int,
                       tpep_pickup_datetime: String,
                       tpep_dropoff_datetime: String,
                       passenger_count: Int,
                       trip_distance: Double,
                       RatecodeID: Int,
                       store_and_fwd_flag: String,
                       PULocationID: Int,
                       DOLocationID: Int,
                       payment_type: Int,
                       fare_amount: Double,
                       extra: Double,
                       mta_tax: Double,
                       tip_amount: Double,
                       tolls_amount: Double,
                       improvement_surcharge: Double,
                       total_amount: Double,
                       tpep_pickup_datetime_hour: Int
                     )

  def add_hours_to_TaxiRide(taxiFactsDF: DataFrame)=
    taxiFactsDF.withColumn("tpep_pickup_datetime_hour", hour(col("tpep_pickup_datetime")))

  def processTaxiFactRDD(taxiFactsDS: Dataset[TaxiRide] )(implicit spark: SparkSession) =
  {

    val taxiFactsRDD: RDD[TaxiRide] =
      taxiFactsDS.rdd

    val mappedTaxiFactRDD: RDD[(Int, Int)] =
      taxiFactsRDD
        //.map(x => (x.tpep_pickup_datetime.substring(11,13).toInt, 1))
        .map(x => (x.tpep_pickup_datetime_hour, 1))

    mappedTaxiFactRDD
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

  }

  def writeTxt(cntRDD: RDD[(Int, Int)], path: String)(implicit spark: SparkSession) =
  {
    FileUtils.deleteQuietly(new File(path))
    cntRDD.coalesce(1).saveAsTextFile(path)
  }

  def main(args: Array[String]) = {
    implicit val spark = SparkSession
      .builder()
      .appName("Homework 5")
      .config("spark.master", "local")
      .getOrCreate()

    //-------------------------------------------------------------------------------------------------
    //Part1
    val taxiFactsDF: DataFrame = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val taxiZoneDF: DataFrame = readCSV("src/main/resources/data/taxi_zones.csv")

    val result: DataFrame = processTaxiData(taxiFactsDF, taxiZoneDF)

    //write to parquet
    writeParquet(result, 1, "src/main/resources/data/yellow_taxi_result")

    //-------------------------------------------------------------------------------------------------
    //Part2

    import spark.implicits._

    val taxiFactsDS_withHour = add_hours_to_TaxiRide(taxiFactsDF).as[TaxiRide]

    val cntRDD = processTaxiFactRDD(taxiFactsDS_withHour)

    writeTxt(cntRDD, "src/main/resources/data/result/hw_3_2")

  }

  //1. Add Time lib
}
