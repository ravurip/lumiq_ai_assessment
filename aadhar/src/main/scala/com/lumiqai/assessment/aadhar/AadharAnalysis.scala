package com.lumiqai.assessment.aadhar

import com.lumiqai.assessment.utils.FilesUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.SaveMode

object AadharAnalysis extends FilesUtil {

  private val appName: String = "aadhar"

  def loadData(): DataFrame = {
    readCSV(appName)
  }

  def checkpoint1(aadhar: DataFrame): Unit = {
    logger.info("~~~~~~~~~~~~~~~~~~~Checkpoint 1~~~~~~~~~~~~~~~~~~~")

    logger.info(s"Aadhar data loaded - counts - ${aadhar.count()}")

    val window = Window.partitionBy(col("private_agency")).orderBy(col("aadhar_generated").desc)

    val df = aadhar.withColumn("rownum", row_number.over(window)).where("rownum <= 25")

    df.coalesce(1).write.mode(SaveMode.Overwrite).csv("files\\aadhar\\checkpoint1.2")
    //awk -F , '{print $3}' part-00000-80f42b27-7645-450b-be7c-3f3e2ea9f736-c000.csv | sort | uniq -c | awk '$1>25 {print $2}'

  }

  def checkpoint2(data: DataFrame): Unit = {
    logger.info("~~~~~~~~~~~~~~~~~~~Checkpoint 2~~~~~~~~~~~~~~~~~~~")

    data.printSchema()

    //Num of registrars in table
    val df = data.select("registrar").groupBy("registrar").agg(count("registrar").as("Count"))
    df.coalesce(1).write.mode(SaveMode.Overwrite).csv("files\\aadhar\\checkpoint2.2")

    logger.info(s"Total Number of states - ${data.select("state").distinct().count()}")

    //num of districts in each state
    data.select("state", "district").distinct()
      .groupBy("state")
      .agg(count(col("district")).cast(IntegerType).as("num_of_districts_in_state"))
      .show(40)

    //Num of Sub districts in each district
    data.select("district", "sub_district").distinct()
      .groupBy("district")
      .agg(count(col("sub_district")).cast(IntegerType).as("num_of_districts_in_state"))
      .show(10000)

    //Private Agencies for each state
    data.select("state", "private_agency").distinct()
      .groupBy("state")
      .agg(collect_list("private_agency").as("agencies_list"))
      .show(40, false)

  }

  def checkpoint3(data: DataFrame): Unit = {
    logger.info("~~~~~~~~~~~~~~~~~~~Checkpoint 3~~~~~~~~~~~~~~~~~~~")

  }

  def checkpoint4(data: DataFrame): Unit = {
    logger.info("~~~~~~~~~~~~~~~~~~~Checkpoint 4~~~~~~~~~~~~~~~~~~~")

    //Unique pincodes in data
    val count = data.select("pincode").distinct.count
    logger.info(s"Total Number of pin codes - $count")

    //Rejected applications in  Maharashtra and UP
    data.filter("state in ('Maharashtra', 'Uttar Pradesh')")
      .select(sum("rejected").cast(IntegerType).as("rejected_counts"))
      .show()

  }

  def checkpoint5(data: DataFrame): Unit = {
    logger.info("~~~~~~~~~~~~~~~~~~~Checkpoint 5~~~~~~~~~~~~~~~~~~~")

  }

  def main(args: Array[String]): Unit = {

    try {

      val aadharData = loadData.cache

      checkpoint1(aadharData)
      checkpoint2(aadharData)
      //      checkpoint3(aadharData)
      checkpoint4(aadharData)
      //      checkpoint5(aadharData)

    } catch {

      case exception: Exception => logger.error("Application failed with error - ", exception)
        throw exception
    } finally {

      spark.stop()
      logger.info("------------------------Spark session stopped and application terminated------------------------")
    }
  }
}
