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
 * Spark program that read,transforms and load the presure data into hive table.
 *
 * @author - Swagat
 */

object PresureDataProcessing extends SparkConfiguration{


  // logger
  val log = Logger.getLogger(getClass.getName)


  /**Swedenweather Presure data transformation and analysis
   *
   * @param sparkSession
   */


  def pressureAnalysis(sparkSession: SparkSession): Unit = {

    try {


      // Reading presure data from Config File

      val presureConfig: Config = ConfigFactory.load("application.conf")

      val presureInput = presureConfig.getString("paths.inputpresure")

      val presureInput1756 = presureConfig.getString("paths.inputpresure1756")

      val presureInput1859 = presureConfig.getString("paths.inputpresure1859")


      // Reading schema from config file

      val presureSchemaFromFile = presureConfig.getString("schema.presureschema")
      val presureSchema = read_schema(presureSchemaFromFile)

      val presureSchemaFromFile1859 = presureConfig.getString("schema.presureschema1859")
      val presureSchema1859 = read_schema(presureSchemaFromFile1859)

      val presureSchemaFromFile1756 = presureConfig.getString("schema.presureschema1756")
      val presureSchema1756 = read_schema(presureSchemaFromFile1756)

      import spark.implicits._

      //-------------------------------Pressure Data (1862 - till current)-------------------------------------------
      val presureRDD = spark.sparkContext
        .textFile(presureInput)
        .map(x => x.split("\\s+"))
        .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4), attributes(5)))

      val presureInputDF = spark.createDataFrame(presureRDD, presureSchema)

      val presureInputDF1 = presureInputDF.withColumn("TEMP_MORN", lit("NaN"))
        .withColumn("PRESURE_REDUCE_MORN", lit("NaN"))
        .withColumn("TEMP_NOON", lit("NaN"))
        .withColumn("PRESURE_REDUCE_NOON", lit("NaN"))
        .withColumn("TEMP_EVEN", lit("NaN"))
        .withColumn("PRESURE_REDUCE_EVEN", lit("NaN"))

      //-------------------------------Pressure Data (1756)-------------------------------------------

      val presureRDD1756 = spark.sparkContext
        .textFile(presureInput1756)
        .map(x => x.split("\\s+"))
        .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))

      val presureDF1756 = spark.createDataFrame(presureRDD1756, presureSchema1756)

      val PresureInputDF2 = presureDF1756
        .withColumn("PRESURE_REDUCE_MORN", lit("NaN"))
        .withColumn("PRESURE_REDUCE_NOON", lit("NaN"))
        .withColumn("PRESURE_REDUCE_EVEN", lit("NaN"))


      //-------------------------------Pressure Data (1859)-------------------------------------------

      val presureRDD1859 = spark.sparkContext
        .textFile(presureInput1859)
        .map(x => x.split("\\s+"))
        .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11)))

      val presureDF1859 = spark.createDataFrame(presureRDD1859, presureSchema1859)

      //Combine all years data and add date column to DF

      val pressureDF = presureDF1859
        .unionByName(presureInputDF1)
        .unionByName(PresureInputDF2)

      val pressureDF1 = pressureDF.withColumn("Date", concat_ws("-", $"YEAR", $"MONTH", $"DAY"))
        .withColumn("DATE", $"Date".cast("Date"))


      //Create hive table with partition on required column and load the data

      SparkCommon.writeToHiveTable(spark,pressureDF1,"presure_parquet")
      log.info("Finished writing to Hive table")


      //---------------------------------------Data Analysis-----------------------------------------

      log.info("Input data count is " + pressureDF1.count())
      log.info("Hive data count " + spark.sql("SELECT count(*) as count FROM presure_parquet").show(false))


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
