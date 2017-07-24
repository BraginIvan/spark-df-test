package com.example.spark.services

import com.example.spark.DecisionMapperTask.spark
import com.example.spark.models.SchemaArgumentsModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.format.DateTimeFormatter

import scala.util.Try

/**
  * Created by ivan on 24.07.17.
  */
object ConvertToUserDefinedFormat {

  def apply(df: DataFrame, newSchemaDesc: List[SchemaArgumentsModel]): DataFrame = {
    val fieldsDesc = newSchemaDesc.map(x => x.existing_col_name -> x).toMap

    def updateRowsToNewSchema(): RDD[Row] = {
      df.rdd.map {
        (oldRow: Row) =>
          val map = oldRow.getValuesMap[String](newSchemaDesc.map(_.existing_col_name))
          val newMap = map.map {
            case (key, value) if value == "null" => key -> null
            case (key, value) =>
              val keySchema = fieldsDesc(key)
              key -> convertValue(value, keySchema.new_data_type, keySchema.dateFormatter)
          }
          Row(newSchemaDesc.map(_.existing_col_name).map(newMap): _*)
      }
    }
    val rowsInNewSchema = updateRowsToNewSchema()
    val newSchema = StructType(newSchemaDesc.map {field => StructField(field.new_col_name, field.dataType)})
    spark.createDataFrame(rowsInNewSchema, newSchema)
  }

  private def convertValue(value: String, newDataType: String, dateExpression: Option[DateTimeFormatter]): Any = {
    newDataType match {
      case "string" => value
      case "integer" => Try(value.toInt).getOrElse(null)
      case "date" =>
        dateExpression
          .flatMap {
            x => Try(x.parseDateTime(value)).toOption }
          .map(d => new java.sql.Date(d.getMillis)).orNull
      case "boolean" => Try(value.toBoolean).getOrElse(null)
      case _ => null
    }
  }



}
