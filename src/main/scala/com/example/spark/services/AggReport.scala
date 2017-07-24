package com.example.spark.services

import com.example.spark.models.{OutputModel, SchemaArgumentsModel}
import org.apache.spark.sql.{DataFrame, Row}
import spray.json._
import scala.collection.JavaConverters._

/**
  * Created by ivan on 24.07.17.
  */
object AggReport {

  private def columnReport(row: Row, column: String): String = {
    val list = row.getList[Any](row.fieldIndex(column)).asScala
    val valuesAsMap = list.groupBy(identity).map { case (key, group) => key.toString -> group.size }
    val values = valuesAsMap.toList.map { case (key, value) => Map(key -> value) }
    val unique_count = valuesAsMap.size
    import com.example.spark.models.OutputModelJsonProtocol._
    OutputModel(Values = values, Unique_values = unique_count, Column = column).toJson.prettyPrint
  }

  def apply(df: DataFrame, newSchemaDesc: List[SchemaArgumentsModel]): Option[String] ={
    import org.apache.spark.sql.functions._
    val columns = newSchemaDesc.map(field => collect_list(field.new_col_name).as(field.new_col_name))
    columns match {
      case head :: tail =>
        df.agg(head, tail: _*).rdd
          .map(row =>
              newSchemaDesc.map(_.new_col_name).map(columnReport(row, _)).mkString("[", ",\n", "]")
          ).collect.headOption
      case Nil => None
    }

  }
}
