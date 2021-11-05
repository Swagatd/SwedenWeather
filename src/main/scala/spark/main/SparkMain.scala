package spark.main

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import spark.utilities.SparkConfiguration
import com.swdenweather.analysis.PresureDataProcessing.pressureAnalysis
import com.swdenweather.analysis.TempDataProcessing.temperatureAnalysis


/**
 * Spark program that transforms temperature data, pressure data and stores it in hive table.
 * This class contains main method, entry point to the spark application.
 *
 * @author - Swagat
 */


object SparkMain extends SparkConfiguration {

  // logger
  val log = Logger.getLogger(getClass.getName)


  //Entry point to the application
  def main(args: Array[String]) {


    // Create SparkSession
    spark.conf.set("spark.app.name", "Pressure and Temperature analysis")

    //pressure data transformation
    log.info("Pressure data transformation and loading started")
    pressureAnalysis(spark)

    //temperature data transformation
    log.info("Temperature data transformation and loading started")
    temperatureAnalysis(spark)

    //stop the spark session
    stopSpark()
  }

}
