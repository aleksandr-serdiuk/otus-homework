package homework3

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import java.io._

object HW3_2 extends App {
  val spark = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val context = spark.sparkContext

  import spark.implicits._

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
                       total_amount: Double
                     )

  val taxiFactsDF: DataFrame =
    spark.read
      .load("src/main/resources/data/yellow_taxi_jan_25_2018")

  val taxiFactsDS: Dataset[TaxiRide] =
    taxiFactsDF
      .as[TaxiRide]

  val taxiFactsRDD: RDD[TaxiRide] =
    taxiFactsDS.rdd

  val mappedTaxiFactRDD: RDD[(Int, Int)] =
    taxiFactsRDD
      .map(x => (x.tpep_pickup_datetime.substring(11,13).toInt, 1))

  mappedTaxiFactRDD

  val cntRDD =
    mappedTaxiFactRDD
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

  //cntRDD.coalesce(1).saveAsTextFile("src/main/resources/data/result/hw_3_2")

  //write to txt file
  val pw = new PrintWriter(new File("src/main/resources/data/result/hw_3_2.txt"))
  cntRDD.foreach(x => {
    println(x)
    val record = x._1.toString() + " " + x._2.toString() + "\n"
    pw.append(record)
  })
  pw.close

}