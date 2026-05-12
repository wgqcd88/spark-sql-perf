package com.wgq

import com.databricks.spark.sql.perf.tpcds.{TPCDS, TPCDSTables}
import org.apache.spark.sql.SparkSession

object Tpcds {

  case class Conf(
      phase: String = "all",
      scaleFactor: String = "1",
      location: String = "",
      format: String = "parquet",
      partitionTables: Boolean = false,
      clusterByPartitionColumns: Boolean = false,
      overwrite: Boolean = true,
      numPartitions: Int = 100,
      database: String = "tpcds",
      dsdgenDir: String = "/opt/tpcds-kit/tools",
      iterations: Int = 1,
      queries: Seq[String] = Seq.empty,
      resultLocation: String = "",
      timeoutSec: Int = 0,
      useDoubleForDecimal: Boolean = false,
      useStringForDate: Boolean = false)

  private def parseArgs(argv: Array[String]): Conf = {
    val kv: Map[String, String] = argv.toList.flatMap {
      case s if s.startsWith("--") =>
        val body = s.stripPrefix("--")
        val (k, v) = body.indexOf('=') match {
          case -1 => (body, "true")
          case i  => (body.substring(0, i), body.substring(i + 1))
        }
        Some(k -> v)
      case _ => None
    }.toMap

    def str(k: String, d: String): String = kv.getOrElse(k, d)
    def boolean(k: String, d: Boolean): Boolean = kv.get(k).map(_.toBoolean).getOrElse(d)
    def int(k: String, d: Int): Int = kv.get(k).map(_.toInt).getOrElse(d)
    def seq(k: String): Seq[String] =
      kv.get(k).map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Seq.empty)

    val phase = str("phase", "all")
    require(Set("generate", "register", "run", "all").contains(phase),
      s"phase must be generate|register|run|all, got: $phase")
    val location = kv.getOrElse("location", sys.error("--location is required"))
    val resultLocation = kv.get("resultLocation").filter(_.nonEmpty)
      .getOrElse(s"${location.stripSuffix("/")}/_results")

    Conf(
      phase = phase,
      scaleFactor = str("scaleFactor", "1"),
      location = location,
      format = str("format", "parquet"),
      partitionTables = boolean("partitionTables", false),
      clusterByPartitionColumns = boolean("clusterByPartitionColumns", false),
      overwrite = boolean("overwrite", true),
      numPartitions = int("numPartitions", 100),
      database = str("database", "tpcds"),
      dsdgenDir = str("dsdgenDir", "/opt/tpcds-kit/tools"),
      iterations = int("iterations", 1),
      queries = seq("queries"),
      resultLocation = resultLocation,
      timeoutSec = int("timeoutSec", 0),
      useDoubleForDecimal = boolean("useDoubleForDecimal", false),
      useStringForDate = boolean("useStringForDate", false))
  }

  def main(argv: Array[String]): Unit = {
    val conf = parseArgs(argv)

    val spark = SparkSession.builder()
      .appName(s"tpcds-${conf.phase}-sf${conf.scaleFactor}")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    println(s"[main] phase=${conf.phase} conf=$conf")

    conf.phase match {
      case "generate" => generate(spark, conf)
      case "register" => register(spark, conf)
      case "run"      => runQueries(spark, conf)
      case "all" =>
        generate(spark, conf)
        register(spark, conf)
        runQueries(spark, conf)
      case other => throw new IllegalArgumentException(s"unknown phase: $other")
    }

    spark.stop()
  }

  private def newTables(spark: SparkSession, c: Conf): TPCDSTables =
    new TPCDSTables(
      sqlContext = spark.sqlContext,
      dsdgenDir = c.dsdgenDir,
      scaleFactor = c.scaleFactor,
      useDoubleForDecimal = c.useDoubleForDecimal,
      useStringForDate = c.useStringForDate)

  def generate(spark: SparkSession, c: Conf): Unit = {
    println(s"[generate] scale=${c.scaleFactor} format=${c.format} location=${c.location} partitionTables=${c.partitionTables}")
    newTables(spark, c).genData(
      location = c.location,
      format = c.format,
      overwrite = c.overwrite,
      partitionTables = c.partitionTables,
      clusterByPartitionColumns = c.clusterByPartitionColumns,
      filterOutNullPartitionValues = false,
      tableFilter = "",
      numPartitions = c.numPartitions)
    println("[generate] done")
  }

  def register(spark: SparkSession, c: Conf): Unit = {
    println(s"[register] database=${c.database} location=${c.location} format=${c.format}")
    newTables(spark, c).createExternalTables(
      location = c.location,
      format = c.format,
      databaseName = c.database,
      overwrite = true,
      discoverPartitions = c.partitionTables)
    println("[register] done")
  }

  def runQueries(spark: SparkSession, c: Conf): Unit = {
    spark.sql(s"USE ${c.database}")

    val tpcds = new TPCDS(spark.sqlContext)
    val all = tpcds.tpcds2_4Queries.filter(q => !q.name.startsWith("ss_max"))

    val selected =
      if (c.queries.isEmpty) all
      else {
        val want = c.queries.map(_.trim).toSet
        val s = all.filter(q => want.contains(q.name.split("-").head))
        val missing = want -- s.map(_.name.split("-").head).toSet
        if (missing.nonEmpty) {
          throw new IllegalArgumentException(s"Unknown queries: ${missing.mkString(",")}")
        }
        s
      }

    println(s"[run] iterations=${c.iterations} queries=${selected.size} resultLocation=${c.resultLocation} timeoutSec=${c.timeoutSec}")

    val status = tpcds.runExperiment(
      executionsToRun = selected,
      iterations = c.iterations,
      resultLocation = c.resultLocation,
      timeout = c.timeoutSec.toLong * 1000L,
      forkThread = false)
    status.waitForFinish(timeoutInSeconds = 24 * 60 * 60)

    val df = status.getCurrentResults()
    df.cache()
    df.show(numRows = 200, truncate = false)
    println(s"[run] results written under ${c.resultLocation}")
  }
}
