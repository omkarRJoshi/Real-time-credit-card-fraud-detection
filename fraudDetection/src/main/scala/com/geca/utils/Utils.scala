package com.geca.utils

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession

object Utils {
  def reader(fileName:String, schema:StructType, sparkSession:SparkSession)  ={
    sparkSession.read
      .option("header", "true")
      .schema(schema)
      .csv(fileName)
  }
  
  def getDistance(lat1:Double, lon1:Double, lat2:Double, lon2:Double) = {
    val r : Int = 6371 // radius of the earth
    val latDistance : Double = Math.toRadians(lat2 - lat1)
    val lonDistance : Double = Math.toRadians(lon2 - lon1)
    val a : Double = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2)
    val c : Double = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    val distance : Double = r * c
    distance
  }
}