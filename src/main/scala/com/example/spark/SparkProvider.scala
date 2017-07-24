package com.example.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by ivan on 24.07.17.
  */
object SparkProvider {
  private val master = "local"
  lazy val spark: SparkSession = SparkSession.builder
    .master(master)
    .appName("DecisionMapper task")
    .getOrCreate
}

trait SparkSupport{
  val spark: SparkSession = SparkProvider.spark
}