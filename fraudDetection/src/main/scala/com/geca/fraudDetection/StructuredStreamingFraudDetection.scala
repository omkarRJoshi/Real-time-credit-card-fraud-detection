package com.geca.fraudDetection

import com.geca.configs.CassandraConfig
import com.geca.configs.SparkConfig
import org.apache.spark.sql.SparkSession
import com.geca.utils.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.geca.creditCard.Schema
import com.geca.creditCard.Enums.TransactionKafka
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel

object StructuredStreamingFraudDetection {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    CassandraConfig.default();
    SparkConfig.defaultSetting();

    val sparkSession = SparkSession.builder().config(SparkConfig.sparkConf).appName("Real time credit card fraud detection").getOrCreate()
    import sparkSession.implicits._
    val customerDf = Utils.readFromCassandra(CassandraConfig.keySpace, CassandraConfig.customer, sparkSession)
    customerDf.show()
    val customerDfWithAge = customerDf.withColumn("age", (datediff(current_date(), to_date($"dob")) / 365).cast(IntegerType));
    customerDfWithAge.cache()

    val transactionStreamDf = getStream(sparkSession)

    val distanceUdf = udf(Utils.getDistance _)

    //    sparkSession.sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = 52428800")
    val processedTransactionDF = transactionStreamDf.join(customerDfWithAge, Seq("cc_num"))
      .withColumn("distance", lit(round(distanceUdf($"lat", $"long", $"merch_lat", $"merch_long"), 2)))
      .select($"cc_num", $"trans_num", to_timestamp($"trans_time", "yyyy-MM-dd HH:mm:ss") as "trans_time", $"category", $"merchant", $"amt", $"merch_lat", $"merch_long", $"distance", $"age")

    val preprocessingModel = PipelineModel.load(SparkConfig.preprocessingModelPath)
    val featureTransactionDF = preprocessingModel.transform(processedTransactionDF)

    val randomForestModel = RandomForestClassificationModel.load(SparkConfig.modelPath)
    val predictionDF = randomForestModel.transform(featureTransactionDF).withColumnRenamed("prediction", "is_fraud")
    //predictionDF.cache

    val fraudPredictionDF = predictionDF.filter($"is_fraud" === 1.0)

    val nonFraudPredictionDF = predictionDF.filter($"is_fraud" =!= 1.0)

    val finalOp = fraudPredictionDF.withColumn("merch_location", struct(predictionDF("merch_lat").alias("lat"), predictionDF("merch_long").alias("lon")))
                              .drop("cc_num_indexed", "merchant_indexed", "distance_indexed", "amt_indexed", "age_indexed", "cc_num_encoded", "category_encoded")
                              .drop("merchant_encoded", "distance_encoded", "amt_encoded", "age_encoded", "features", "rawPrediction", "probability", "merch_lat", "merch_long")
                              
//    finalOp.printSchema()
    finalOp.writeStream.format("console").outputMode("append").start().awaitTermination()
//    finalOp.selectExpr("to_json(struct(*)) AS value")
//      .writeStream.format("kafka")
//      .outputMode("append")
//      .option("topic", "fraud_detection")
//      .option("kafka.bootstrap.servers", "192.168.43.116:9092")
//      .option("checkpointLocation", "/home/omkar/Desktop/checkpoint")
//      .start()
//      .awaitTermination()
    sparkSession.stop()
  }

  def getStream(sparkSession: SparkSession) = {
    import sparkSession.implicits._

    /* https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html */
    val kafkaStream = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.1.12:9092")
      .option("group.id", "cons2")
      .option("subscribe", "test")
      .option("startingOffsets", "latest")
      .load()
    val transactionString = kafkaStream.selectExpr("CAST(value AS STRING)")
    val jsonString = transactionString.select(from_json(col("value"), Schema.kafkaSchema).alias("transaction"))
    val transactionStreamDf = jsonString.select("transaction.*")
      .withColumn("amt", lit($"amt") cast (DoubleType))
      .withColumn("merch_lat", lit($"merch_lat") cast (DoubleType))
      .withColumn("merch_long", lit($"merch_long") cast (DoubleType))
      .withColumn("trans_time", lit($"trans_time") cast (TimestampType))
      .drop("first")
      .drop("last")
    transactionStreamDf
  }
}