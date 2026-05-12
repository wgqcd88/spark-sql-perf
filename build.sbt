name := "spark-sql-perf"

organization := "com.databricks"

scalaVersion := "2.12.18"

crossScalaVersions := Seq("2.12.18")

licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

val sparkVersion = "3.5.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
  "com.twitter"      %% "util-jvm"    % "6.45.0" % Provided,
  "org.yaml"          % "snakeyaml"   % "1.23",
  "org.scalatest"    %% "scalatest"   % "3.2.18" % Test
)

fork := true

Compile / console / initialCommands :=
  """
    |import org.apache.spark.sql._
    |import org.apache.spark.sql.functions._
    |import org.apache.spark.sql.types._
    |import org.apache.spark.sql.hive.test.TestHive
    |import TestHive.implicits
    |import TestHive.sql
    |
    |val sqlContext = TestHive
    |import sqlContext.implicits._
  """.stripMargin
