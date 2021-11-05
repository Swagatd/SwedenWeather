import java.io.FileNotFoundException
import scala.util.Failure
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import com.typesafe.config.{Config, ConfigFactory}
import Presurefunction.read_schema

object  PresureDataIngestAndRefine {

  val log = Logger.getLogger(getClass.getName)


  def main(args: Array[String]): Unit = {

    try {

      val spark = SparkSession.builder
        .master("local")
        .appName("PresureDataIngestAndRefine")
        .getOrCreate()
      val sc = spark.sparkContext

      // Reading presure data from Config File

      val presureconfig: Config = ConfigFactory.load("application.conf")

      val presureinput = presureconfig.getString("paths.inputpresure")

      val presureinput1756 = presureconfig.getString("paths.inputpresure1756")

      val presureinput1859 = presureconfig.getString("paths.inputpresure1859")


      // Reading schema from config file

      val PresureSchemaFromFile = presureconfig.getString("schema.presureschema")
      val PresureSchema = read_schema(PresureSchemaFromFile)

      val PresureSchemaFromFile1859 = presureconfig.getString("schema.presureschema1859")
      val PresureSchema1859 = read_schema(PresureSchemaFromFile1859)

      val PresureSchemaFromFile1756 = presureconfig.getString("schema.presureschema1756")
      val PresureSchema1756 = read_schema(PresureSchemaFromFile1756)

      import spark.implicits._

      //-------------------------------Pressure Data (1862 - till current)-------------------------------------------
      val PresureRDD = sc
        .textFile(presureinput)
        .map(x => x.split("\\s+"))
        .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4), attributes(5)))

      val PresureinputDF = spark.createDataFrame(PresureRDD, PresureSchema)

      val PresureinputDF1 = PresureinputDF.withColumn("TEMP_MORN", lit("NaN"))
        .withColumn("PRESURE_REDUCE_MORN", lit("NaN"))
        .withColumn("TEMP_NOON", lit("NaN"))
        .withColumn("PRESURE_REDUCE_NOON", lit("NaN"))
        .withColumn("TEMP_EVEN", lit("NaN"))
        .withColumn("PRESURE_REDUCE_EVEN", lit("NaN"))

      //-------------------------------Pressure Data (1756)-------------------------------------------

      val PresureRDD1756 = sc
        .textFile(presureinput1756)
        .map(x => x.split("\\s+"))
        .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))

      val PresureDF1756 = spark.createDataFrame(PresureRDD1756, PresureSchema1756)

      val PresureinputDF2 = PresureDF1756
        .withColumn("PRESURE_REDUCE_MORN", lit("NaN"))
        .withColumn("PRESURE_REDUCE_NOON", lit("NaN"))
        .withColumn("PRESURE_REDUCE_EVEN", lit("NaN"))


      //-------------------------------Pressure Data (1859)-------------------------------------------

      val PresureRDD1859 = sc
        .textFile(presureinput1859)
        .map(x => x.split("\\s+"))
        .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11)))

      val PresureDF1859 = spark.createDataFrame(PresureRDD1859, PresureSchema1859)

      //Combine all years data and add date column to DF

      val pressureDF = PresureDF1859
        .unionByName(PresureinputDF1)
        .unionByName(PresureinputDF2)

      val pressureDF1 = pressureDF.filter(col("YEAR") === "1756")
        .withColumn("Date", concat_ws("-", $"YEAR", $"MONTH", $"DAY"))
        .withColumn("DATE", $"Date".cast("Date"))


      // Mysql connectivity

      pressureDF1.write.format("jdbc")
        .options(Map(
          "url" -> "jdbc:mysql://localhost:3306/swedenweather",
          "driver" -> "com.mysql.jdbc.Driver",
          "dbtable" -> "presure",
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

