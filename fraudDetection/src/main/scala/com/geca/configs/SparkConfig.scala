package com.geca.configs

//import com.geca.configs.CassandraConfig
import org.apache.log4j.Logger
import org.apache.spark.SparkConf

object SparkConfig {
  val logger = Logger.getLogger(getClass.getName);
  val sparkConf = new SparkConf
  
  var transactionDatasouce:String = _
  var customerDatasource:String = _
  var modelPath:String = _
  var preprocessingModelPath:String = _
  var shutdownMarker:String = _
  var batchInterval:Int = _
  
  def load(){
    logger.info("loading spark setting")
    
  }
  
  def defaultSetting(){
    sparkConf.setMaster("local[*]")
             .set("spark.cassandra.connection.host", CassandraConfig.cassandraHost);
//    shutdownMarker = "/tmp/shutdownmarker"
    transactionDatasouce = "/home/omkar/workspace/beProject/real-time-credit-card-fraud-detection/data/transactions.csv"
    customerDatasource = "/home/omkar/workspace/beProject/real-time-credit-card-fraud-detection/data/customer.csv"
    modelPath = "/home/omkar/workspace/beProject/real-time-credit-card-fraud-detection/fraudDetection/models/randomForestModel"
    preprocessingModelPath = "/home/omkar/workspace/beProject/real-time-credit-card-fraud-detection/fraudDetection/models/preprocessingModel"
    batchInterval = 5000
  }
}