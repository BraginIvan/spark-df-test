package com.example.spark

import com.example.spark.models.SchemaArgumentsModel
import com.example.spark.services.{AggReport, ConvertToUserDefinedFormat, FilterWithEmptyStrings}
import org.apache.spark.sql.DataFrame
import spray.json._
import com.example.spark.models.SchemaArgumentsModelJsonProtocol._

/**
  * Created by ivan on 24.07.17.
  */
object DecisionMapperTask extends SparkSupport{


  def main(args: Array[String]): Unit = {
    process(args, "")
  }

  def process(args: Array[String], filePath: String): Option[String] ={
    val df: DataFrame = CSVReader.getCsvAsDF(filePath)
    val filteredEmptyStrings = FilterWithEmptyStrings(df)
    val newSchemaDesc = args.toList.map(_.parseJson.convertTo[SchemaArgumentsModel])
    val dfNewSchema = ConvertToUserDefinedFormat(filteredEmptyStrings, newSchemaDesc)
    val report = AggReport(dfNewSchema, newSchemaDesc)
    report.foreach(println)
    report
  }

}
