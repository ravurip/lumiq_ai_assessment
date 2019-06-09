package com.lumiqai.assessment.enterprisesurvey

import com.lumiqai.assessment.utils.FilesUtil
import org.apache.spark.sql.functions.col

object EnterpriseSurvey extends FilesUtil {

  private val appName: String = "enterpriseSurvey"

  def main(args: Array[String]): Unit = {

    logger.info("--Application Started--")

    try {

      val df = readCSV(appName).cache()
      logger.info(s"Loaded $appName data - counts: ${df.count()}")

      writeParquet(appName, "file", df.filter(col("corrupt_record").isNull))
      logger.info(s"Successfully written $appName good data in parquet form")

      writeParquet(appName, "badData", df.filter(col("corrupt_record").isNotNull))
      logger.info(s"Successfully written $appName bad data in parquet form")

    } catch {
      case exception: Exception =>
        logger.error("Application failed with Error: ", exception)
        throw exception

    } finally {
      spark.stop()
      logger.info("------------------------Spark session stopped and application terminated------------------------")
    }

  }

}