package com.swdenweather.analysis

import org.apache.spark.sql.types.{StringType, StructType}



/**
 * Scala object used to read the schema from application.conf file and
 * create a schema of StructType which will be used to read input data
 *
 * @author - Swagat
 */


object PresureFunction {


  /**Read the schema of string type and return it in StructType
   *
   * @param schema_arg
   * @return
   */

  def read_schema(schema_arg : String):StructType = {

    var sch : StructType = new StructType

    val split_values = schema_arg.split(",").toList

    val d_types = Map(

      "StringType" -> StringType

    )

    for (i<- split_values){
      val columnVal = i.split(" ").toList

      sch = sch.add(columnVal(0),d_types(columnVal(1)),nullable = true  )

    }
    // Return the Schema
    sch


  }

}
