import org.apache.spark.sql.types.{StringType, StructType}

object Presurefunction {

  def read_schema(schema_arg : String) = {

    var sch : StructType = new StructType

    val split_values = schema_arg.split(",").toList

    val d_types = Map(

      "StringType" -> StringType


    )

    for (i<- split_values){
      val columnVal = i.split(" ").toList

      sch = sch.add(columnVal(0),d_types(columnVal(1)),nullable = true  )


    }
    sch

  }

}
