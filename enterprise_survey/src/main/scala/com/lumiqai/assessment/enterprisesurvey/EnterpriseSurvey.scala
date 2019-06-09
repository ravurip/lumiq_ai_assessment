package com.lumiqai.assessment.enterprisesurvey

import com.lumiqai.assessment.utils.FilesUtil
import org.apache.spark.sql.functions.col

object EnterpriseSurvey extends FilesUtil {

  private val appName: String = "enterpriseSurvey"

  def main(args: Array[String]): Unit = {

    logger.info("--Application Started--")

    try {

      val df = readCSV(appName).cache()

      writeParquet(appName, "file", df.filter(col("corrupt_record").isNull))
      writeParquet(appName, "badData", df.filter(col("corrupt_record").isNotNull))

    } catch {
      case exception: Exception =>
        logger.error("Application failed with Error: ", exception)
        throw exception

    } finally {
      spark.stop()
      logger.info("Application terminated")
    }

  }

}