package com.example.spark

import com.example.spark.DecisionMapperTask.spark

/**
  * Created by ivan on 24.07.17.
  */
object CSVReader extends SparkSupport{

  def getCsvAsDF(path: String) = spark.read
    .format("csv")
    .option("header", "true")
    .csv(path)
}
