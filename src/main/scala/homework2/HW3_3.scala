import org.apache.spark.sql.functions.{broadcast, col, count, desc_nulls_first, max, mean, min, round}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

object HW3_3 extends App {

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

  def store_to_pstgrs (df: DataFrame, url: String, table: String) = {
    df.write.format("jdbc")
      .mode(SaveMode.Append)
      .option("url", url)
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", table)
      .option("driver", "org.postgresql.Driver")
      .save()
  }

  val table_name = "result_yellow_taxi"

  Try(
    store_to_pstgrs(result, "jdbc:postgresql://localhost:5432/postgres", table_name)
  ) match {
    case Success(_) => println(s"Stored to ${table_name}")
    case Failure(exception) => println(exception)
  }

}
