package com.example.spark.models

import org.apache.spark.sql.types._
import org.joda.time.format.DateTimeFormatter
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

/**
  * Created by ivan on 24.07.17.
  */
case class SchemaArgumentsModel(existing_col_name: String,
                                new_col_name: String,
                                new_data_type: String,
                                date_expression: Option[String]){
  def dataType: DataType =
    new_data_type match {
      case "string" => StringType
      case "integer" => IntegerType
      case "date" => DateType
      case "boolean" => BooleanType
      case _ => StringType
    }

  def dateFormatter: Option[DateTimeFormatter] =
    date_expression.map(org.joda.time.format.DateTimeFormat.forPattern)
}

object SchemaArgumentsModelJsonProtocol extends DefaultJsonProtocol{
  implicit val schemaArgumentsModelJson: RootJsonFormat[SchemaArgumentsModel] =
    jsonFormat4(SchemaArgumentsModel.apply)
}
