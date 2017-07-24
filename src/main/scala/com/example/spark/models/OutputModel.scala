package com.example.spark.models

import org.apache.spark.sql.types._
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

/**
  * Created by ivan on 24.07.17.
  */
case class OutputModel(Column: String,
                       Unique_values: Int,
                       Values: List[Map[String, Int]])

object OutputModelJsonProtocol extends DefaultJsonProtocol{
  implicit val outputModelJson: RootJsonFormat[OutputModel] =
    jsonFormat3(OutputModel.apply)
}