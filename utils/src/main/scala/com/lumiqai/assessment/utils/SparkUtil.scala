package com.lumiqai.assessment.utils

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

trait SparkUtil extends ConfigReader {

  val runMode: String = config.getString("runMode.mode")

  lazy val spark: SparkSession = {
    try {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val spark = SparkSession
        .builder()
        .master(runMode)
        .getOrCreate()
      spark.conf.set("spark.sql.columnNameOfCorruptRecord", "corrupt_record")

      spark
    } catch {
      case exception: Exception => {
        logger.error("Error initialising SparkSession")
        throw exception
      }
    }
  }
}
