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

  private val parser = new scopt.OptionParser[Conf]("Tpcds") {
    head("com.wgq.Tpcds", "0.1.0")
    opt[String]("phase")
      .validate(v =>
        if (Set("generate", "register", "run", "all").contains(v)) success
        else failure("phase must be generate|register|run|all"))
      .action((v, c) => c.copy(phase = v))
      .text("generate|register|run|all (default: all)")
    opt[String]("scaleFactor").action((v, c) => c.copy(scaleFactor = v))
      .text("TPC-DS scale factor, default 1")
    opt[String]("location").required().action((v, c) => c.copy(location = v))
      .text("data root, e.g. abfss://warehouse@sa.dfs.core.windows.net/tpcds_sf1")
    opt[String]("format").action((v, c) => c.copy(format = v))
      .text("parquet|orc, default parquet")
    opt[Boolean]("partitionTables").action((v, c) => c.copy(partitionTables = v))
    opt[Boolean]("clusterByPartitionColumns").action((v, c) => c.copy(clusterByPartitionColumns = v))
    opt[Boolean]("overwrite").action((v, c) => c.copy(overwrite = v))
    opt[Int]("numPartitions").action((v, c) => c.copy(numPartitions = v))
      .text("Spark write partitions, default 100")
    opt[String]("database").action((v, c) => c.copy(database = v))
      .text("Hive database to register, default tpcds")
    opt[String]("dsdgenDir").action((v, c) => c.copy(dsdgenDir = v))
      .text("path to dsdgen, default /opt/tpcds-kit/tools")
    opt[Int]("iterations").action((v, c) => c.copy(iterations = v))
      .text("query iterations, default 1")
    opt[Seq[String]]("queries").action((v, c) => c.copy(queries = v))
      .text("comma-separated query subset (e.g. q1,q14a). Default: all 99.")
    opt[String]("resultLocation").action((v, c) => c.copy(resultLocation = v))
      .text("where to write timing JSON; default <location>/_results")
    opt[Int]("timeoutSec").action((v, c) => c.copy(timeoutSec = v))
      .text("per-query timeout, 0 = no timeout")
    opt[Boolean]("useDoubleForDecimal").action((v, c) => c.copy(useDoubleForDecimal = v))
    opt[Boolean]("useStringForDate").action((v, c) => c.copy(useStringForDate = v))
    help("help")
  }

  def main(argv: Array[String]): Unit = {
    val conf = parser.parse(argv, Conf()) match {
      case Some(c) =>
        val rl = if (c.resultLocation.isEmpty) s"${c.location.stripSuffix("/")}/_results" else c.resultLocation
        c.copy(resultLocation = rl)
      case None => sys.exit(2)
    }

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
