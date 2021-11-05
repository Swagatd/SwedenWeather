package com.swdenweather.analysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Logger

/**
 * This will create hive table and load the data using dataframe passed.
 *
 * @author - Swagat
 */

object SparkCommon {

  // logger
  val log = Logger.getLogger(getClass.getName)

  def writeToHiveTable(spark:SparkSession,df:DataFrame,hiveTable : String): Unit ={
    log.warn("writeToHiveTable Started")

    val tmpView = hiveTable + "temp"
    df.createOrReplaceTempView(tmpView)

    val sqlQuery = "create table" + hiveTable + "as select * from presureTbl " +
    "stored as parquet location '/data/" + hiveTable + "/"

    spark.sql(sqlQuery)

    log.warn("Finished Writing to Hive Table")




  }

}
