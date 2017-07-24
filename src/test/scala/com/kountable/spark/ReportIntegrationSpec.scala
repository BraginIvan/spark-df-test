package com.kountable.spark

import com.example.spark.DecisionMapperTask
import org.scalatest.{Matchers, WordSpecLike}

/**
  * Created by ivan on 24.07.17.
  */
class ReportIntegrationSpec extends WordSpecLike with Matchers{

  val expected = """[{
    |  "Column": "first_name",
    |  "Unique_values": 1,
    |  "Values": [{
    |    "John": 1
    |  }]
    |},
    |{
    |  "Column": "total_years",
    |  "Unique_values": 1,
    |  "Values": [{
    |    "26": 2
    |  }]
    |},
    |{
    |  "Column": "d_o_b",
    |  "Unique_values": 2,
    |  "Values": [{
    |    "1996-01-26": 1
    |  }, {
    |    "1995-01-26": 2
    |  }]
    |}]""".stripMargin.replaceAll(" ", "")

  val args = Array("{\"existing_col_name\":\"name\",\"new_col_name\":\"first_name\",\"new_data_type\":\"string\"}",
    "{\"existing_col_name\":\"age\",\"new_col_name\":\"total_years\",\"new_data_type\":\"integer\"}",
    "{\"existing_col_name\":\"birthday\",\"new_col_name\":\"d_o_b\",\"new_data_type\":\"date\",\"date_expression\":\"dd-MM-yyy\"}")

  "the process" must{
    "create report" in{
      DecisionMapperTask.process(args, "./src/test/resources/sample.csv").map(_.replaceAll(" ", "")) should contain (expected)
    }
  }
}
