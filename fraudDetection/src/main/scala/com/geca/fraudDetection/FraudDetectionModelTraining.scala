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

object FraudDetectionModelTraining {
  def main(args:Array[String]){
    CassandraConfig.default()                 
    SparkConfig.defaultSetting() 
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession.builder().config(SparkConfig.sparkConf).appName("Basic ml job").getOrCreate()
    val file:String = "/home/omkar/Desktop/wine-data.csv";
    
     val schemaStruct = StructType(
            StructField("country", StringType) ::
            StructField("points", DoubleType) ::
            StructField("price", DoubleType) :: Nil
     )
     
    val data = sparkSession.read.option("header", true).schema(schemaStruct).csv(file).na.drop()
    data.show()
    val Array(training, testData) = data.randomSplit(Array(.7,.3))
    val labelCol = "price"
    
    val countryIndexer = new StringIndexer().setInputCol("country").setOutputCol("countryIndex").setHandleInvalid("keep")
    val assembler = new VectorAssembler().setInputCols(Array("points", "countryIndex")).setOutputCol("features")
    val lr = new LinearRegression().setLabelCol(labelCol).setFeaturesCol("features").setPredictionCol("predicted " + labelCol)
    
    val stages = Array(countryIndexer, assembler, lr)
    val pipeline = new Pipeline().setStages(stages)
    val model = pipeline.fit(training)
    val prediction = model.transform(testData)
    prediction.show()
    val evaluator = new RegressionEvaluator()
            .setLabelCol(labelCol)
            .setPredictionCol("predicted " + labelCol)
            .setMetricName("rmse")
        //We compute the error using the evaluator
        val error = evaluator.evaluate(prediction)
        println(error)
    sparkSession.stop()
  }
}