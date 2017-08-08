package dengunator

/* MobilityModel.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.databricks.spark.avro._
import org.apache.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

object MobilityModel {
  def main(args: Array[String]) {
    val spark_host = "sineth-HP-ProBook-450-G1"
    val host = "localhost"
    val hdfs_port = 9000
    val sqlContext = new SQLContext(new SparkContext(new SparkConf().setAppName("MobilityModel Application")
      .setMaster("spark://" + spark_host + ":7077")))

    // Read MOH to cellId mapping. Headers[cellid, TowerIndex]
    val cellIndexTowerIndexSchema = StructType(Array(
      StructField("lat", StringType, nullable = true),
      StructField("lon", StringType, nullable = true)))

    val cellIndexTowerIndexDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .schema(cellIndexTowerIndexSchema)
      .load("hdfs://" + host + ":" + hdfs_port + "/user/sparkdata/tower_locations_minified.csv").cache()
    cellIndexTowerIndexDf.registerTempTable("cellIndexTowerIndexView")

    // Read date to week number mapping. Headers[CALL_DATE, WeekNumber]
    // val dateWeekMapSchema = StructType(Array(
    //   StructField("CALL_DATE", StringType, nullable = true),
    //   StructField("WeekNumber", StringType, nullable = true)))

    // val dateWeekMapDf = sqlContext.read
    //   .format("com.databricks.spark.csv")
    //   .option("header", "true") // Use first line of all files as header
    //   .schema(dateWeekMapSchema)
    //   .load("hdfs://" + host + ":" + hdfs_port + "/data/resources/day_weekNum_map.csv").cache()
    // dateWeekMapDf.registerTempTable("dateWeekMapView")

    // //--- Process CDR start---//

    // val cdrSchema = StructType(Array(
    //   StructField("C0", StringType, nullable = true),
    //   StructField("DEVICE_NAME", StringType, nullable = true),
    //   StructField("C2", StringType, nullable = true),
    //   StructField("C3", StringType, nullable = true),
    //   StructField("C4", StringType, nullable = true),
    //   StructField("C5", StringType, nullable = true),
    //   StructField("DURATION", StringType, nullable = true)))

    // // Read CDR data
    // val cdrDf = sqlContext.read
    //   .format("com.databricks.spark.csv")
    //   .option("header", "false") // Use first line of all files as header
    //   .option("delimiter", "|")
    //   .schema(cdrSchema)
    //   .load("hdfs://" + host + ":" + hdfs_port + "/data/voice/VOICE20130110*")
    // cdrDf.registerTempTable("cdrView")

    // /* Select only necessary colums from CDR. Headers[CALL_DIRECTION_KEY, ANUMBER, OTHER_NUMBER, cellid, CALL_DATE,
    //      * CALL_Time]
    //      * CALL_DATE, CALL_Time is taken from CALL_TIME colum(YYYY-MM-DD:HH:MM:SS) in CDR*/
    // val reducedColumCdrDf = sqlContext.sql("""Select C0 CALL_DIRECTION_KEY, C2 ANUMBER,
    //   C3 OTHER_NUMBER, C4 cellid, SUBSTRING(C5,5,4) CALL_DATE, SUBSTRING(C5,9,4) CALL_Time
    //   FROM cdrView""")
    // reducedColumCdrDf.registerTempTable("reducedColumCdrView")

    // // Select ANUMBER as the subscriberId relevant to cellid when CALL_DIRECTION_KEY=2
    // val outCdrDf = sqlContext.sql("""SELECT cellid, CALL_DATE, CALL_Time, ANUMBER subId
    //                                 FROM reducedColumCdrView where CALL_DIRECTION_KEY=2""")
    // outCdrDf.registerTempTable("outCdrDfView")

    // // Select OTHER_NUMBER as the subscriberId relevant to cellid when CALL_DIRECTION_KEY=1
    // val inCdrDf = sqlContext.sql("""SELECT cellid, CALL_DATE, CALL_Time, OTHER_NUMBER subId
    //                               FROM reducedColumCdrView where CALL_DIRECTION_KEY=1""")
    // inCdrDf.registerTempTable("inCdrDfView")

    // // Vertically join above two table to get complete CDR data set.Headers[cellid, CALL_DATE, CALL_Time, subId]
    // val cdrCompackedDf = sqlContext.sql("SELECT * FROM outCdrDfView UNION ALL SELECT * FROM inCdrDfView")
    // cdrCompackedDf.registerTempTable("cdrCompackedView")
    // //--- Process CDR end---//

    // //--- Combine MOH and week number with CDR data start---//
    // /* Join Compacked CDR table and cellIndexMOH to get MOH name for each CDR record.
    //     	 Headers[cellid, CALL_DATE, CALL_Time, subId, TowerIndex]*/
    // val cdrWithTowerIndexDf = sqlContext.sql("""SELECT cdrCompackedView.cellid cellid, subId, TowerIndex, CALL_Time,CALL_DATE
    //   FROM cdrCompackedView LEFT JOIN cellIndexTowerIndexView
    //   ON cdrCompackedView.cellid = cellIndexTowerIndexView.cellid""")
    // cdrWithTowerIndexDf.registerTempTable("cdrWithTowerIndexView")

    // // Only select CDR with at night(2130-0530) to find home MOH. Headers[subId, TowerIndex]
    // val cdrAtNightDf = sqlContext.sql("""SELECT cellid, subId, TowerIndex FROM cdrWithTowerIndexView
    //                                         WHERE CALL_Time>2130 OR CALL_Time<0530""")
    // cdrAtNightDf.registerTempTable("cdrAtNightDf")

    // /*CDR with week number. join cdrWithTowerIndexView and dateWeekMapView table to add week number to each record.
    //       Headers[subId, TowerIndex, WeekNumber]*/
    // val cdrWithWeekDf = sqlContext.sql("""SELECT subId, TowerIndex, WeekNumber, cdrWithTowerIndexView.CALL_DATE
    //     									FROM cdrWithTowerIndexView left join dateWeekMapView
    //     										on cdrWithTowerIndexView.CALL_DATE=dateWeekMapView.CALL_DATE""")
    // cdrWithWeekDf.registerTempTable("cdrWithWeekView")
    // //--- Combine MOH and week number with CDR data end---//

    // //--- Calculate home MOH of each subscriber start---//
    // /*TowerIndex is MOH name, callCountInMOH is the count of calls made by the subscriber in each MOH at night(1900-0600)
    //        Group all the records by subscriberId subId first and then by TowerIndex.Then get a count.
    //        Headers[subscriberId, TowerIndex, callCountInMOH]*/
    // val callsInMOHDf = sqlContext.sql("""SELECT subId subscriberId, TowerIndex, count(subId) as callCountInMOH
    //   FROM cdrAtNightDf GROUP BY subId,TowerIndex""")
    // callsInMOHDf.registerTempTable("callsInMOHView")

    // /* Find most frequent MOH of each subscriber by number of calls made at night.
    //  * There can be more than one Most Frequent MOH when two MOHs have equal number of callCountInMOH.
    //  * So the first most frequent MOH has been taken as the home MOH since subscriber should only have one home MOH.
    //  */
    // /* Self joined callsInMOHView to get the MOH of the subscriber with the maximum number of call count.
    //       Headers[subscriberId, TowerIndex, callCountInMOH]*/
    // val SubscriberFrqntMOHDf = sqlContext.sql("""SELECT a.subscriberId, a.TowerIndex, a.callCountInMOH FROM callsInMOHView a
    //     INNER JOIN (SELECT subscriberId, MAX(callCountInMOH) callCountInMOH FROM callsInMOHView GROUP BY subscriberId) b
    //     ON a.subscriberId = b.subscriberId AND a.callCountInMOH = b.callCountInMOH""")
    // SubscriberFrqntMOHDf.registerTempTable("SubscriberFrqntMOHView")

    // /* Self joined SubscriberFrqntMOHView to get the first MOH when there multiple frequent MOHs for a subscriber.
    //       Headers[subscriberId, homeMOH] */
    // val SubscriberHomeMOHDF = sqlContext.sql("""SELECT a.subscriberId, b.homeMOH FROM SubscriberFrqntMOHView a
    //                            INNER JOIN (SELECT subscriberId, FIRST(TowerIndex) as homeMOH, FIRST(callCountInMOH)
    //                            as callCountInMOH FROM SubscriberFrqntMOHView GROUP BY subscriberId) AS b
    //                            ON a.subscriberId = b.subscriberId AND a.TowerIndex = b.homeMOH""")
    // SubscriberHomeMOHDF.registerTempTable("SubscriberHomeMOHView")
    // //--- Calculate home MOH of each subscriber end---//

    // //--- Calculate Mobility start---//
    // /* The mobility value for a MOH is calculated by aggregating number of calls made in that particular MOH
    //     * by subscribers whose Home MOH is not the same.The average fraction of time that users living in
    //     * MOH i spend in MOH j during a week is calculated.
    //     * Assumption: The number of phone calls made by a user while in MOH j
    //     * is proportional to the time spent there
    //     */

    // /* Modified CDR record by adding home MOH for each record. join cdrWithWeekView with SubscriberHomeMOHView
    //     * on subscriberId to add home MOH to each record.
    //     * Headers[subId, TowerIndex, homeMOH, WeekNumber]*/
    // val modifiedCdrDf = sqlContext.sql("""SELECT subId, TowerIndex, homeMOH, WeekNumber FROM cdrWithWeekView
    //                                    inner join SubscriberHomeMOHView
    //                                    on cdrWithWeekView.subId = SubscriberHomeMOHView.subscriberId""")
    // modifiedCdrDf.registerTempTable("modifiedCdrView")

    // /* callsAwayFromHome is the number of calls made by the subscriber in other MOH areas(not Home)
    //     * Group all the records by TowerIndex first and then by Home MOH of the subscriber homeMOH, and then by the week.
    //     * Then get a count.
    //     * Headers[subId, TowerIndex, homeMOH, WeekNumber, callsAwayFromHome]*/
    // val awayCallCountDF = sqlContext.sql("""SELECT TowerIndex, homeMOH, WeekNumber, count(TowerIndex) callsAwayFromHome
    //                                        FROM modifiedCdrView group by TowerIndex, homeMOH, WeekNumber""")
    // awayCallCountDF.registerTempTable("awayCallCountView")

    // // Remove calls within the home MOH when calculating mobility
    // val mobilityDf = sqlContext.sql("SELECT * FROM awayCallCountView WHERE TowerIndex<>homeMOH")
    // mobilityDf.registerTempTable("mobilityView")

    /* Group all the records by TowerIndex first and then by WeekNumber.Then get a sum.
          Headers[TowerIndex, WeekNumber, FractionOfCalls]*/
    // sqlContext.sql("""SELECT TowerIndex, WeekNumber, sum(callsAwayFromHome) FractionOfCalls FROM mobilityView group by TowerIndex,
    //      WeekNumber order by WeekNumber, TowerIndex""")
    //   .write.format("com.databricks.spark.csv") 
    //   .save("hdfs://" + host + ":" + hdfs_port + "/user/hadoop/lf_job5")
    //--- Calculate Mobility end---//

    sqlContext.sql("""select count(*) from cellIndexTowerIndexView""")
    .write.format("com.databricks.spark.csv")
    .save("hdfs://" + host + ":" + hdfs_port + "/user/output/file")
  }
}
