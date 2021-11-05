import java.io.FileNotFoundException
import PresureDataIngestAndRefine.{getClass, log}
import scala.util.Failure
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.typesafe.config.{Config, ConfigFactory}
import Presurefunction.read_schema


object TemperatureDataProcessing {

  val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    try {
      val spark = SparkSession.builder
        .master("local")
        .appName("TemperatureDataProcessing")
        .getOrCreate()
      val sc = spark.sparkContext


      // Reading temperature data from Config File

      val tempconfig: Config = ConfigFactory.load("application.conf")

      val tempinput = tempconfig.getString("paths.inputtemperature")

      val tempinput1756 = tempconfig.getString("paths.inputtemperature1756")

      val tempinput1859 = tempconfig.getString("paths.inputtemperature1859")


      // Reading schema from config file

      val tempSchemaFromFile = tempconfig.getString("schema.temperatureschema")
      val tempSchema = read_schema(tempSchemaFromFile)

      val tempSchemaFromFile1859 = tempconfig.getString("schema.temperatureschema1859")
      val tempSchema1859 = read_schema(tempSchemaFromFile1859)

      val tempSchemaFromFile1756 = tempconfig.getString("schema.temperatureschema1756")
      val tempSchema1756 = read_schema(tempSchemaFromFile1756)

      import spark.implicits._

      //-------------------------------Temperature Data (1862 - till current)-------------------------------------------
      val TempRDD = sc
        .textFile(tempinput)
        .map(x => x.split("\\s+"))
        .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))

      val TempinputDF = spark.createDataFrame(TempRDD, tempSchema)


      //-------------------------------Temperature Data (1859)-------------------------------------------

      val TempRDD1859 = sc
        .textFile(tempinput1859)
        .map(x => x.split("\\s+"))
        .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7)))

      val TempinputDF1859 = spark.createDataFrame(TempRDD1859, tempSchema1859)

      val TempinputDF1 = TempinputDF1859
        .withColumn("TEMPERATURE_MEAN", lit("NaN"))


      //-------------------------------Temperature Data (1756)-------------------------------------------

      val TempRDD1756 = sc
        .textFile(tempinput1756)
        .map(x => x.split("\\s+"))
        .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6)))

      val TempinputDF1756 = spark.createDataFrame(TempRDD1756, tempSchema1756).drop("EXTRA")

      val TempinputDF2 = TempinputDF1756
        .withColumn("TEMPERATURE_MIN", lit("NaN"))
        .withColumn("TEMPERATURE_MAX", lit("NaN"))
        .withColumn("TEMPERATURE_MEAN", lit("NaN"))


      //Combine all years data and add date column to DF

      val TemperatureDF = TempinputDF
        .unionByName(TempinputDF2)
        .unionByName(TempinputDF1)

      val TemperatureDF1 = TemperatureDF.filter(col("YEAR") === "1756")
        .withColumn("Date", concat_ws("-", $"YEAR", $"MONTH", $"DAY"))
        .withColumn("DATE", $"Date".cast("Date"))
      TemperatureDF1.show(10, false)

      // Mysql connectivity

      TemperatureDF1.write.format("jdbc")
        .options(Map(
          "url" -> "jdbc:mysql://localhost:3306/swedenweather",
          "driver" -> "com.mysql.jdbc.Driver",
          "dbtable" -> "temperature",
          "user" -> "root"
          //      "password" -> "swedenweather"
        ))
        .mode("append")
        .save()


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
