package com.geca.fraudDetection

import org.apache.spark.sql.SparkSession
import com.geca.configs.SparkConfig
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import com.geca.creditCard.Schema
import com.geca.configs.CassandraConfig
import org.apache.spark.sql.types.IntegerType
import com.geca.utils.Utils

object InitialImportToCassandra {
  def main(args : Array[String]){
    
    CassandraConfig.default()                 
    SparkConfig.defaultSetting()    
    val sparkSession = SparkSession.builder().config(SparkConfig.sparkConf).appName("cassandra importer").getOrCreate()
    import sparkSession.implicits._
    
    val transactionDf = Utils.reader(SparkConfig.transactionDatasouce, Schema.fruadCheckedTransactionSchema, sparkSession)
                        .withColumn("trans_date", split($"trans_date", "T").getItem(0))
                        .withColumn("trans_time", concat_ws(" ", $"trans_date", $"trans_time"))
                        .withColumn("trans_time", to_timestamp($"trans_time", "YYYY-MM-dd HH:mm:ss") cast(TimestampType))
  
    val customerDf = Utils.reader(SparkConfig.customerDatasource, Schema.customerSchema, sparkSession)
    
    val customerAgeDf = customerDf.withColumn("age", (datediff(current_date(), to_date($"dob")) / 365 ).cast(IntegerType))
    
    val distanceUDF = udf(Utils.getDistance _)
    
    val processedDF = transactionDf.join(broadcast(customerAgeDf), Seq("cc_num"))
      .withColumn("distance", lit(round(distanceUDF($"lat", $"long", $"merch_lat", $"merch_long"), 2)))
      .select("cc_num", "trans_num", "trans_time", "category", "merchant", "amt", "merch_lat", "merch_long", "distance", "age", "is_fraud")
    
    processedDF.cache()
    val fraudTransactionDf = processedDF.filter($"is_fraud" === 1)
    val nonFraudTransactionDf = processedDF.filter($"is_fraud" === 0)
    
    // writing data to cassandra
    customerDf.write.format("org.apache.spark.sql.cassandra")
              .mode("append")
              .options(Map("keyspace" -> CassandraConfig.keySpace, "table" -> CassandraConfig.customer))
              .save()
              
    fraudTransactionDf.write.format("org.apache.spark.sql.cassandra")
              .mode("append")
              .options(Map("keyspace" -> CassandraConfig.keySpace, "table" -> CassandraConfig.fraudTransactionTable))
              .save()  
              
    nonFraudTransactionDf.write.format("org.apache.spark.sql.cassandra")
              .mode("append")
              .options(Map("keyspace" -> CassandraConfig.keySpace, "table" -> CassandraConfig.nonFraudTransactionTable))
              .save()
    
  }
}