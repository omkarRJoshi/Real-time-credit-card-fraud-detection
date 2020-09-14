package com.geca.configs

import org.apache.log4j.Logger

object CassandraConfig {
  val logger = Logger.getLogger(getClass.getName);
  
  var keySpace:String = _
  var fraudTransactionTable:String = _
  var nonFraudTransactionTable:String = _
  var customer:String = _
  var cassandraHost:String = _
  
  def load(){
    logger.info("Loading cassandra settings. ")
  }
  
  def default(){
    keySpace = "creditcard"
    fraudTransactionTable = "fraud_transaction"
    nonFraudTransactionTable = "non_fraud_transaction"
    customer = "customer"
    cassandraHost = "localhost"
  }
  
  
}