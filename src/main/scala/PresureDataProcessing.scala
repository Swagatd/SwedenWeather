import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import com.typesafe.config.{Config, ConfigFactory}
import Presurefunction.read_schema

object  PresureDataIngestAndRefine {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName("WeatherDataAnalysis")
      .config("spark.some.config.option" , "some-value")
      .getOrCreate()
    val sc = spark.sparkContext

    // Reading presure data from Config File

    val presureconfig : Config = ConfigFactory.load("application.conf")

    val presureinput = presureconfig.getString("paths.inputpresure")

    val presureinput1756 = presureconfig.getString("paths.inputpresure1756")

    val presureinput1859 = presureconfig.getString("paths.inputpresure1859")


    // Reading schema from config

    val PresureSchemaFromFile = presureconfig.getString("schema.presureschema")
    val PresureSchema = read_schema(PresureSchemaFromFile)

    val PresureSchemaFromFile1859 = presureconfig.getString("schema.presureschema1859")
    val PresureSchema1859 = read_schema(PresureSchemaFromFile1859)

    val PresureSchemaFromFile1756 = presureconfig.getString("schema.presureschema1756")
    val PresureSchema1756 = read_schema(PresureSchemaFromFile1756)

    import  spark.implicits._

//    val PresureSchema = StructType(List(
//      StructField("YEAR", StringType,true),
//      StructField("MONTH", StringType,true),
//      StructField("DAY", StringType,true),
//      StructField("PRESURE_MORN", StringType,true),
//      StructField("PRESURE_NOON", StringType,true),
//      StructField("PRESURE_EVEN", StringType,true)
//    ))

//    val PresureSchema1859 = StructType(List(
//      StructField("YEAR", StringType,true),
//      StructField("MONTH", StringType,true),
//      StructField("DAY", StringType,true),
//      StructField("PRESURE_MORN", StringType,true),
//      StructField("TEMP_MORN", StringType,true),
//      StructField("PRESURE_REDUCE_MORN", StringType,true),
//      StructField("PRESURE_NOON", StringType,true),
//      StructField("TEMP_NOON", StringType,true),
//      StructField("PRESURE_REDUCE_NOON", StringType,true),
//      StructField("PRESURE_EVEN", StringType,true),
//      StructField("TEMP_EVEN", StringType,true),
//      StructField("PRESURE_REDUCE_EVEN", StringType,true)
//
//    ))


//    val PresureSchema1756 = StructType(List(
//      StructField("YEAR", StringType,true),
//      StructField("MONTH", StringType,true),
//      StructField("DAY", StringType,true),
//      StructField("PRESURE_MORN", StringType,true),
//      StructField("TEMP_MORN", StringType,true),
//      StructField("PRESURE_NOON", StringType,true),
//      StructField("TEMP_NOON", StringType,true),
//      StructField("PRESURE_EVEN", StringType,true),
//      StructField("TEMP_EVEN", StringType,true)
//
//
//    ))


    val PresureRDD = sc
      .textFile(presureinput)
//      .textFile("F:\\Scala Project\\Data\\air_presure_general\\stockholm*.txt")
      .map(x => x.split("\\s+"))
      .map(attributes => Row(attributes(0),attributes(1),attributes(2), attributes(3),attributes(4),attributes(5)))

    val PresureinputDF = spark.createDataFrame(PresureRDD, PresureSchema)

    val PresureinputDF1 = PresureinputDF.withColumn("TEMP_MORN", lit("NaN"))
      .withColumn("PRESURE_REDUCE_MORN", lit("NaN"))
      .withColumn("TEMP_NOON", lit("NaN"))
      .withColumn("PRESURE_REDUCE_NOON", lit("NaN"))
      .withColumn("TEMP_EVEN", lit("NaN"))
      .withColumn("PRESURE_REDUCE_EVEN", lit("NaN"))

    //    PresureRDD.take(10).foreach(println)

//    PresureinputDF1.show(10,false)
//    PresureDF.printSchema()

    //-------------------------------Pressure Data (1756)-------------------------------------------

    val PresureRDD1756 = sc
      .textFile(presureinput1756)
      .map(x => x.split("\\s+"))
      .map(x => Row(x(0),x(1),x(2), x(3),x(4),x(5), x(6),x(7),x(8)))

    val PresureDF1756 = spark.createDataFrame(PresureRDD1756, PresureSchema1756)

    val PresureinputDF2 = PresureDF1756
      .withColumn("PRESURE_REDUCE_MORN", lit("NaN"))
      .withColumn("PRESURE_REDUCE_NOON", lit("NaN"))
      .withColumn("PRESURE_REDUCE_EVEN", lit("NaN"))

    //    PresureRDD.take(10).foreach(println)

//    PresureDF1756.show(10,false)
//    PresureDF1756.printSchema()


    //-------------------------------Pressure Data (1859)-------------------------------------------

    val PresureRDD1859 = sc
      .textFile(presureinput1859)
      .map(x => x.split("\\s+"))
      .map(x => Row(x(0),x(1),x(2), x(3),x(4),x(5), x(6),x(7),x(8),x(9),x(10),x(11)))

    val PresureDF1859 = spark.createDataFrame(PresureRDD1859, PresureSchema1859)

    //    PresureRDD.take(10).foreach(println)

//    PresureDF1859.show(10,false)
//
//    PresureDF1859.printSchema()


    val pressureDF = PresureDF1859
      .unionByName(PresureinputDF1)
      .unionByName(PresureinputDF2)

    val pressureDF1 = pressureDF.filter(col("YEAR") === "1756")
              .withColumn("Date",concat_ws("-",$"YEAR",$"MONTH",$"DAY"))
              .withColumn("DATE",$"Date".cast("Date"))
//              .show(10,false)

    // Mysql connectivity

    pressureDF1.write.format("jdbc")
      .options(Map(
        "url" -> "jdbc:mysql://localhost:3306/swedenweather",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "presure",
        "user" -> "root"
        //        "password" -> "gkcodelabs"
      ))
      .mode("append")
      .save()



  }

}