package dengunator

/* MobilityModel.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.databricks.spark.avro._
import org.apache.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.apache.log4j._
import java.util.Calendar
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD


object MobilityModel {
  def main(args: Array[String]) {
//    val spark_host = "sineth-HP-ProBook-450-G1"
//    val host = "localhost"
//    val hdfs_port = 9000
//    val sqlContext = new SQLContext(new SparkContext(new SparkConf().setAppName("Spark Mobility Model 12")
//      .setMaster("spark://" + spark_host + ":7077")))
//    val log = LogManager.getRootLogger
//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)
//    log.setLevel(Level.INFO)
//    // Read MOH to cellId mapping. Headers[cellid, TowerIndex]
//    val cellIndexTowerIndexSchema = StructType(Array(
//      StructField("lat", StringType, nullable = true),
//      StructField("lon", StringType, nullable = true)))
//
//    /*
//    load the celltowerindex
//     */
//    val cellIndexTowerIndexDf = sqlContext.read
//      .format("com.databricks.spark.csv")
//      .option("header", "true") // Use first line of all files as header
//      .schema(cellIndexTowerIndexSchema)
//      .load("hdfs://" + host + ":" + hdfs_port + "/user/sparkdata/tower_locations_minified.csv").cache()
//    cellIndexTowerIndexDf.registerTempTable("cellIndexTowerIndexView")
//
//    val cdrSchema = StructType(Array(
//      StructField("C0", StringType, nullable = true),
//      StructField("DEVICE_NAME", StringType, nullable = true),
//      StructField("C2", StringType, nullable = true),
//      StructField("C3", StringType, nullable = true),
//      StructField("C4", StringType, nullable = true),
//      StructField("C5", StringType, nullable = true),
//      StructField("DURATION", StringType, nullable = true)))
//
//    // Read CDR data
//    val cdrDf = sqlContext.read
//      .format("com.databricks.spark.csv")
//      .option("header", "false") // Use first line of all files as header
//      .option("delimiter", "|")
//      .schema(cdrSchema)
//      .load("hdfs://" + host + ":" + hdfs_port + "/user/sparkdata/voice_sample_20130501_20130514_size_0.01.csv")
//
//    /*
//    Extracting the WEEK and the YEAR from the CDR timestamp for aggregating the number of call records
//    for a tower in a given week for a given year
//     */
//    cdrDf.withColumn("yearAndWeek", date_format(unix_timestamp(col("C5"), "yyyyMMddHHmmss").cast("timestamp"), "yyyy-MM-dd"))
//      .withColumn("WEEK", weekofyear(col("yearAndWeek")))
//      .withColumn("YEAR", year(col("yearAndWeek")))
//      .drop("C5", "yearAndWeek")
//      .registerTempTable("cdrView")
//
//    val reducedColumnCdrDf = sqlContext.sql(
//      """Select C0 AS CALL_DIRECTION_KEY, C2 AS ANUMBER, C3 AS OTHER_NUMBER, C4 AS cellid, WEEK, YEAR
//       FROM cdrView""")
//    reducedColumnCdrDf.registerTempTable("reducedColumnCdrView")
//
//
//    val towerDf = sqlContext.read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .option("delimiter", ",")
//      .load("hdfs://" + host + ":" + hdfs_port + "/user/sparkdata/indexed_towers.csv")
//
//    towerDf.join(reducedColumnCdrDf, "cellid").registerTempTable("towerIndexCDRView")
//    /*
//    Query to get the aggregate of the CDR records for a tower given a WEEK and a YEAR
//     */
//    val dataExtractionQuery = sqlContext.sql("""select first(latitude) AS LAT,first(longitude) AS LON, cellid, count(*) AS COUNT,WEEK, YEAR from towerIndexCDRView group by YEAR,WEEK,cellid""")
//
//    /*
//      Saving the DataExtractionQuery
//     */
//    val formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
//    val current = formatter.format(Calendar.getInstance().getTime)
//    dataExtractionQuery.write.format("com.databricks.spark.csv")
//      .option("header", "true")
//      .save("hdfs://" + host + ":" + hdfs_port + "/user/output/"+current+"dataExtractionQuery")
//

    /*
    Query to calculate the probability matrix
    todo : the query seems to run ok (100% CPU) but produces no results. WHY?

    val query = sqlContext.sql("""select cellid,count(*) from towerIndexCDRView group by YEAR,WEEK,cellid limit 50""")

    val probMatrixSourceRDD = query.rdd

    val resultantProbRDD = probMatrixSourceRDD.cartesian(probMatrixSourceRDD).collect {
      case (t1: (String, Long), t2: (String, Long)) if t1 != t2 => (t1._1, t2._1, t1._2 * t2._2)
    }

    resultantProbRDD.saveAsTextFile("hdfs://" + host + ":" + hdfs_port + "/user/output/" + current)
    */

//    val query = sqlContext.sql("""select cellid,count(*),YEAR,WEEK from towerIndexCDRView group by YEAR,WEEK,cellid limit 50""")
//
//    val probMatrixSourceRDD = query.rdd
//
//    val resultantProbRDD = probMatrixSourceRDD.cartesian(probMatrixSourceRDD).collect {
//      case (t1: Row, t2: Row) if t1 != t2 => (t1.getString(0), t2.getString(0), t1.getLong(1) * t2.getLong(1))
//    }
//
//    resultantProbRDD.saveAsTextFile("hdfs://" + host + ":" + hdfs_port + "/user/output/" + current)
//
//
//    query.join(query)
    val spark_host = "sineth-HP-ProBook-450-G1"
    val host = "localhost"
    val hdfs_port = 9000
    val sqlContext = new SQLContext(new SparkContext(new SparkConf().setAppName("Spark Mobility Model 12")
      .setMaster("spark://" + spark_host + ":7077")))
    val log = LogManager.getRootLogger
//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)
//    log.setLevel(Level.INFO)

    val outputDir = "uom_17_job_3"
    val spark = SparkSession.builder()
      .appName("Spark UOM Mobility Model")
      .master("spark://" + spark_host + ":7077")
      .getOrCreate()

    val cdrSchema = StructType(Array(
      StructField("C0", StringType, nullable = true),
      StructField("DEVICE_NAME", StringType, nullable = true),
      StructField("C2", StringType, nullable = true),
      StructField("C3", StringType, nullable = true),
      StructField("C4", StringType, nullable = true),
      StructField("C5", StringType, nullable = true),
      StructField("DURATION", StringType, nullable = true)))

    // Read CDR data
    val cdrDf = spark.read
      .option("header", "false") // Use first line of all files as header
      .option("delimiter", "|")
      .schema(cdrSchema)
      .csv("hdfs://" + host + ":" + hdfs_port + "/user/sparkdata/voice_sample_20130501_20130514_size_0.01.csv")

    /*
    Extracting the WEEK and the YEAR from the CDR timestamp for aggregating the number of call records
    for a tower in a given week for a given year
     */
    cdrDf.withColumn("yearAndWeek", date_format(unix_timestamp(col("C5"), "yyyyMMddHHmmss").cast("timestamp"), "yyyy-MM-dd"))
      .withColumn("WEEK", weekofyear(col("yearAndWeek")))
      .withColumn("YEAR", year(col("yearAndWeek")))
      .drop("C5", "yearAndWeek")
      .createOrReplaceTempView("cdrView")

    val reducedColumnCdrDf = spark.sql(
      """Select C0 AS CALL_DIRECTION_KEY, C2 AS ANUMBER, C3 AS OTHER_NUMBER, C4 AS cellid, WEEK, YEAR
       FROM cdrView""")
    reducedColumnCdrDf.createOrReplaceTempView("reducedColumnCdrView")

    val towerDf = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("hdfs://" + host + ":" + hdfs_port + "/user/sparkdata/indexed_towers.csv")

    towerDf.join(reducedColumnCdrDf, "cellid").createOrReplaceTempView("towerIndexCDRView")
    /*
        Query to calculate the probability matrix
    */
    val callCountPerCellDf = spark.sql("""select YEAR,WEEK,cellid as CELL_ID,count(*) as CALL_COUNT from towerIndexCDRView group by YEAR,WEEK,cellid limit 100""")
    val otherCallCountDf = callCountPerCellDf.toDF("OTHER_YEAR", "OTHER_WEEK", "OTHER_CELL_ID", "OTHER_CALL_COUNT")
    val resultantDf = callCountPerCellDf.join(otherCallCountDf, col("YEAR") === col("OTHER_YEAR") && col("WEEK") === col("OTHER_WEEK")
      && col("CELL_ID") =!= col("OTHER_CELL_ID"))
    val probMatrixDf = resultantDf.withColumn("MOV_VAL", col("CALL_COUNT") * col("OTHER_CALL_COUNT"))
      .select("YEAR", "WEEK", "CELL_ID", "OTHER_CELL_ID", "MOV_VAL")

    probMatrixDf.write.csv("hdfs://" + host + ":" + hdfs_port + "/user/output/" + outputDir)
  }

}
