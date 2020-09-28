package com.geca.algorithms

import org.apache.spark.sql._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.classification.RandomForestClassifier

object MlAlgorithms {
  def dataBalanceWithKMean(df: DataFrame, reductionCount: Long, sparkSession: SparkSession) = {
    val kmeans = new KMeans().setK(reductionCount.toInt).setMaxIter(30)
    val model = kmeans.fit(df)

    import sparkSession.implicits._
    model.clusterCenters.toList.map(v => (v, 0)).toDF("features", "label")
  }

  def randomForestClassifier(df: DataFrame, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))
    val randomForestEstimator = new RandomForestClassifier()
                                                .setLabelCol("label")
                                                .setFeaturesCol("features")
                                                .setMaxBins(7000)
    val model = randomForestEstimator.fit(df)
    val transactionWithPrediction = model.transform(df)
    println()
    println(s"total data count is" + transactionWithPrediction.count())
    println("count of same label " + transactionWithPrediction.filter($"prediction" === $"label").count())
    model
  }
}