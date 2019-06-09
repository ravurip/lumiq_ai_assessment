package com.lumiqai.assessment.utils

import java.util.Date

import org.apache.hadoop.fs.Path
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructType}

trait FilesUtil extends SparkUtil {

  private def renameFile(filePath: String): Unit = {
    logger.info("Checking whether the file already exists or not.")

    try {

      val source = new Path(filePath)

      val conf = spark.sparkContext.hadoopConfiguration
      val fs = source.getFileSystem(conf)

      if (fs.exists(source)) {

        val time = new Date().getTime

        val timestamp: String = time.toString
        val destination = new Path(filePath + "_" + timestamp)

        logger.info(s"File Already exists. Taking the backup for the file. Destination: $destination")

        if (fs.rename(source, destination)) {
          logger.info("File renamed successfully")
        } else {
          throw new Exception
        }
      }

    } catch {
      case ex: Exception => {
        logger.error(s"Error in renaming the file $filePath")
        throw ex
      }
    }
  }

  def getConfObject(app: String): Config = {
    config.getConfig(s"$app")
  }

  def getStructType(schema: String): StructType = {

    var struct = new StructType()

    schema
      .split(",")
      .foreach { column =>
        val field = column.split("-")
        struct = struct.add(field(0).trim, field(1))
      }

    struct.add("corrupt_record", StringType)
  }

  def readCSV(app: String): DataFrame = {

    val conf = getConfObject(s"$app.csv")

    val a: String = conf.getString("schema")

    val structType: StructType = getStructType(a)

    spark.read
      .option("header", conf.getBoolean("header"))
      .option("delimiter", conf.getString("delimiter"))
      .option("mode", conf.getString("mode"))
      .option("multiLine", conf.getString("multiLine"))
      .schema(structType)
      .csv(conf.getString("file"))
  }


  def writeParquet(app: String, data: DataFrame): Unit = {

    val conf = getConfObject(s"$app.parquet")

  }

}
