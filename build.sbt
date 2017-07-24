name := "spark-task"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies += "io.spray" %% "spray-json" % "1.3.3"

libraryDependencies += "joda-time" % "joda-time" % "2.9.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"
