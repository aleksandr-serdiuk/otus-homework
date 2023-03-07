package homework5

import homework5.hw5.{processTaxiFactRDD, TaxiRide, readParquet, add_hours_to_TaxiRide}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class test1_AnyFlatSpec extends AnyFlatSpec {


  implicit val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Test №1 for Big Data Application")
    .getOrCreate()

  it should "upload and process data" in {
    val taxiDF = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    import spark.implicits._
    val taxiFactsDS_withHour = add_hours_to_TaxiRide(taxiDF).as[TaxiRide]
    val res = processTaxiFactRDD(taxiFactsDS_withHour)

    assert(taxiFactsDS_withHour.columns.size == 18, "Should be 18 columns in taxiFactsDS_withHour")
    assert(res.count() == 24, "Should be 24 rows in result by hours")

  }

}
