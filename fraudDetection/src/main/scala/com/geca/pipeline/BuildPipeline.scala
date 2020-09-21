package com.geca.pipeline

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.VectorAssembler
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.PipelineStage

object BuildPipeline {
  def create(columns:List[String]):ListBuffer[PipelineStage] = {
     val featureColumns : ListBuffer[String] = ListBuffer()
     val stages : ListBuffer[PipelineStage] = ListBuffer()
     
     val arr = columns.map(col => {
        val stringIndexer = new StringIndexer()
        stringIndexer.setInputCol(col).setOutputCol(col + "_indexed")
            
        stages += stringIndexer 
    })
    val arr2 = columns.map(col => {
        val oneHotEncoder = new OneHotEncoder()
        oneHotEncoder.setInputCol(col + "_indexed").setOutputCol(col + "_encoded")
        
        featureColumns += (col + "_encoded")
        stages += oneHotEncoder
    })
    val vectorAssembler =  {
        new VectorAssembler()
        .setInputCols(featureColumns.toArray).setOutputCol("features")
    }
    stages += vectorAssembler
    stages
  }
}