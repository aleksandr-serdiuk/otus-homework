package homework5

import homework5.hw5.{processTaxiData, readCSV, readParquet}
import org.apache.spark.sql.QueryTest.{checkAnswer}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{Row, SQLContext, SQLImplicits, SparkSession}

class test2_SharedSparkSession extends SharedSparkSession  {

  test("Test2-1") {

    val taxiFactsDF = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val taxiZoneDF = readCSV("src/main/resources/data/taxi_zones.csv")

    val actualDistribution = processTaxiData(taxiFactsDF, taxiZoneDF)

    checkAnswer(
      actualDistribution.limit(1),
      Row("Manhattan", 296527, 0.0, 2.18, 37.92) :: Nil
    )

    checkAnswer(
      taxiFactsDF.groupBy("vendorID").count(),
        Row(1, 147554) ::
        Row(2, 184339) :: Nil
    )
  }

}
