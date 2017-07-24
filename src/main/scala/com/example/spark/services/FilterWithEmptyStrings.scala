package com.example.spark.services

import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by ivan on 24.07.17.
  */
object FilterWithEmptyStrings {
  private val emptyStringRegexp = "\\s*"
  def apply(df: DataFrame): DataFrame ={// did not find how to filter by empty "" string because in spark 2.0.1 and later "" is null
    def getAllRowColumns(row: Row) ={
      val columnsN = row.size
      (0 until columnsN).map(row.get)
    }
    df.filter{row =>
      val hasEmptyString = getAllRowColumns(row).exists {
        case s: String if s.matches(emptyStringRegexp) => true
        case _ => false
      }
      !hasEmptyString
    }
  }
}
