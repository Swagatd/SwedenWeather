import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.{lit,col}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.typesafe.config.{Config,ConfigFactory}
import Presurefunction.read_schema



object TemperatureDataProcessing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName("WeatherDataAnalysis")
      .config("spark.some.config.option" , "some-value")
      .getOrCreate()
    val sc = spark.sparkContext


    // Reading temperature data from Config File

    val tempconfig : Config = ConfigFactory.load("application.conf")

    val tempinput = tempconfig.getString("paths.inputtemperature")

    val tempinput1756 = tempconfig.getString("paths.inputtemperature1756")

    val tempinput1859 = tempconfig.getString("paths.inputtemperature1859")


    // Reading schema from config

    val tempSchemaFromFile = tempconfig.getString("schema.temperatureschema")
    val tempSchema = read_schema(tempSchemaFromFile)

    val tempSchemaFromFile1859 = tempconfig.getString("schema.temperatureschema1859")
    val tempSchema1859 = read_schema(tempSchemaFromFile1859)

    val tempSchemaFromFile1756 = tempconfig.getString("schema.temperatureschema1756")
    val tempSchema1756 = read_schema(tempSchemaFromFile1756)

    import  spark.implicits._



    val TempRDD = sc
      .textFile(tempinput)
      //      .textFile("F:\\Scala Project\\Data\\air_presure_general\\stockholm*.txt")
      .map(x => x.split("\\s+"))
      .map(x => Row(x(0),x(1),x(2), x(3),x(4),x(5), x(6),x(7),x(8)))

    val TempinputDF = spark.createDataFrame(TempRDD, tempSchema)

//    TempinputDF.show(10,false)
//    TempinputDF.printSchema()



    //-------------------------------Temperature Data (1756)-------------------------------------------

    val TempRDD1859 = sc
      .textFile(tempinput1859)
      .map(x => x.split("\\s+"))
      .map(x => Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7)))

    val TempinputDF1859 = spark.createDataFrame(TempRDD1859, tempSchema1859)

    val TempinputDF1 = TempinputDF1859
      .withColumn("TEMPERATURE_MEAN", lit("NaN"))


//    TempinputDF1859.show(10,false)
//    TempinputDF1859.printSchema()

    //-------------------------------Temperature Data (1756)-------------------------------------------

    val TempRDD1756 = sc
      .textFile(tempinput1756)
      .map(x => x.split("\\s+"))
      .map(x => Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6)))

    val TempinputDF1756 = spark.createDataFrame(TempRDD1756, tempSchema1756).drop("EXTRA")

    val TempinputDF2 = TempinputDF1756
      .withColumn("TEMPERATURE_MIN", lit("NaN"))
      .withColumn("TEMPERATURE_MAX", lit("NaN"))
      .withColumn("TEMPERATURE_MEAN", lit("NaN"))

//        TempinputDF1756.show(10,false)
//        TempinputDF1756.printSchema()



    val TemperatureDF = TempinputDF
      .unionByName(TempinputDF2)
      .unionByName(TempinputDF1)

    val TemperatureDF1 = TemperatureDF.filter(col("YEAR") === "1756")
                 .withColumn("Date",concat_ws("-",$"YEAR",$"MONTH",$"DAY"))
                 .withColumn("DATE",$"Date".cast("Date"))
    TemperatureDF1.show(10,false)

//    // Mysql connectivity
//
//    TemperatureDF1.write.format("jdbc")
//      .options(Map(
//        "url" -> "jdbc:mysql://localhost:3306/swedenweather",
//        "driver" -> "com.mysql.jdbc.Driver",
//        "dbtable" -> "temperature",
//        "user" -> "root"
////        "password" -> "gkcodelabs"
//      ))
//      .mode("append")
//      .save()



  }

}
