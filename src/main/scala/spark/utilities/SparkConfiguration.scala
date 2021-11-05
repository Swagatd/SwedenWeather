package spark.utilities

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
 * Trait to start and stop Spark session
 *
 * @author - Swagat
 *
 */

trait SparkConfiguration {

  //Define a spark session
  lazy val spark =
    SparkSession.builder
      .master("local")
      .enableHiveSupport()
      .appName("PresureandTemperatureDataProcessing")
      .getOrCreate()



  //Method to stop the spark session
  def stopSpark(): Unit = spark.stop()
}


