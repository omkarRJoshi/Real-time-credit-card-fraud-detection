package com.geca.fraudDetection

import org.apache.spark.sql.SparkSession
import com.geca.configs.SparkConfig
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.Pipeline
import com.geca.configs.CassandraConfig
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.geca.utils.Utils
import com.geca.pipeline.BuildPipeline
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.PipelineStage
import scala.collection.mutable.ListBuffer
import com.geca.algorithms.MlAlgorithms

object FraudDetectionModelTraining {
  def main(args:Array[String]){
    CassandraConfig.default()                 
    SparkConfig.defaultSetting() 
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession.builder().config(SparkConfig.sparkConf).appName("Train models for fraud detection").getOrCreate()
    
    import sparkSession.implicits._
    
    val fraudTransactionDf = Utils.readFromCassandra(CassandraConfig.keySpace, CassandraConfig.fraudTransactionTable, sparkSession)
                                  .select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud")
    val nonFraudTransactionDf = Utils.readFromCassandra(CassandraConfig.keySpace, CassandraConfig.nonFraudTransactionTable, sparkSession)
                                  .select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud")   
                                  
    val transactionDf = fraudTransactionDf.union(nonFraudTransactionDf)
    transactionDf.cache()
    
    val columnNames = List("cc_num", "category", "merchant", "distance", "amt", "age")
    
    val pipelineStages = BuildPipeline.create(columnNames)

    val pipeline = new Pipeline().setStages(pipelineStages.toArray)
    val preprocessingModel = pipeline.fit(transactionDf)//.transform(transactionDf)
    
    println()
    println("Saving preprocessing model")
    preprocessingModel.save(SparkConfig.preprocessingModelPath)
    val featureDf = preprocessingModel.transform(transactionDf)
//    preprocessingModel.printSchema()
//    preprocessingModel.show()
    
    val fraudDf = featureDf.filter($"is_fraud" === 1)
                                    .withColumnRenamed("is_fraud", "label")
                                    .select("features", "label")
    val nonFraudDf = featureDf.filter($"is_fraud" =!= 1)
    val fraudCount = fraudDf.count()
    
    val balancedNonFraudDf = MlAlgorithms.dataBalanceWithKMean(nonFraudDf, fraudCount, sparkSession)
    val finalFeatureDf = fraudDf.union(balancedNonFraudDf)
    
    val randomForestModel = MlAlgorithms.randomForestClassifier(finalFeatureDf, sparkSession)
    
    println()
    println("Saving random forest model")
    
    randomForestModel.save(SparkConfig.modelPath)
    sparkSession.stop()
  }
}