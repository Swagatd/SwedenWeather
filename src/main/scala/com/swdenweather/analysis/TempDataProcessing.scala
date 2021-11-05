package com.swdenweather.analysis

import java.io.FileNotFoundException

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import PresureFunction.read_schema
import spark.utilities.SparkConfiguration

import scala.util.Failure


/**
 * Spark program that read,transforms and load the Temperature data into hive table.
 *
 * @author - Swagat
 */

object TempDataProcessing extends SparkConfiguration {
  val log = Logger.getLogger(getClass.getName)


  def temperatureAnalysis(sparkSession: SparkSession): Unit = {

    try {

      // Reading temperature data from Config File

      val tempConfig: Config = ConfigFactory.load("application.conf")

      val tempInput = tempConfig.getString("paths.inputtemperature")

      val tempInput1756 = tempConfig.getString("paths.inputtemperature1756")

      val tempInput1859 = tempConfig.getString("paths.inputtemperature1859")


      // Reading schema from config file

      val tempSchemaFromFile = tempConfig.getString("schema.temperatureschema")
      val tempSchema = read_schema(tempSchemaFromFile)

      val tempSchemaFromFile1859 = tempConfig.getString("schema.temperatureschema1859")
      val tempSchema1859 = read_schema(tempSchemaFromFile1859)

      val tempSchemaFromFile1756 = tempConfig.getString("schema.temperatureschema1756")
      val tempSchema1756 = read_schema(tempSchemaFromFile1756)

      import spark.implicits._

      //-------------------------------Temperature Data (1862 - till current)-------------------------------------------
      val tempRDD = spark.sparkContext
        .textFile(tempInput)
        .map(x => x.split("\\s+"))
        .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))

      val tempInputDF = spark.createDataFrame(tempRDD, tempSchema)


      //-------------------------------Temperature Data (1859)-------------------------------------------

      val tempRDD1859 = spark.sparkContext
        .textFile(tempInput1859)
        .map(x => x.split("\\s+"))
        .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7)))

      val tempInputDF1859 = spark.createDataFrame(tempRDD1859, tempSchema1859)

      val tempInputDF1 = tempInputDF1859
        .withColumn("TEMPERATURE_MEAN", lit("NaN"))


      //-------------------------------Temperature Data (1756)-------------------------------------------

      val tempRDD1756 = spark.sparkContext
        .textFile(tempInput1756)
        .map(x => x.split("\\s+"))
        .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6)))

      val tempInputDF1756 = spark.createDataFrame(tempRDD1756, tempSchema1756).drop("EXTRA")

      val tempInputDF2 = tempInputDF1756
        .withColumn("TEMPERATURE_MIN", lit("NaN"))
        .withColumn("TEMPERATURE_MAX", lit("NaN"))
        .withColumn("TEMPERATURE_MEAN", lit("NaN"))


      //Combine all years data and add date column to DF

      val temperatureDF = tempInputDF
        .unionByName(tempInputDF2)
        .unionByName(tempInputDF1)

      val temperatureDF1 = temperatureDF.withColumn("Date", concat_ws("-", $"YEAR", $"MONTH", $"DAY"))
        .withColumn("DATE", $"Date".cast("Date"))


      //Create hive table with partition on required column and load the data

      SparkCommon.writeToHiveTable(spark,temperatureDF1,"temperature_parquet")
      log.info("Finished writing to Hive table")


      //---------------------------------------Data Analysis-----------------------------------------

      log.info("Input data count is " + temperatureDF1.count())
      log.info("Hive data count " + spark.sql("SELECT count(*) as count FROM temperature_parquet").show(false))


    }
    catch {
      case fileNotFoundException: FileNotFoundException => {
        log.error("Input file not found")
        Failure(fileNotFoundException)
      }
      case exception: Exception => {
        log.error("Exception found " + exception)
        Failure(exception)
      }
    }

  }

}
