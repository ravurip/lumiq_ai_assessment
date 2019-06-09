package com.lumiqai.assessment.utils

import java.util.Date

import org.apache.hadoop.fs.Path
import com.typesafe.config.{Config, ConfigException}
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.catalyst.parser.ParseException

trait FilesUtil extends SparkUtil {

  private def renameFile(filePath: String): Unit = {
    try {

      val source = new Path(filePath)

      val conf = spark.sparkContext.hadoopConfiguration
      val fs = source.getFileSystem(conf)

      logger.info("Checking if the file already exists")

      if (fs.exists(source)) {

        val time = new Date().getTime.toString

        val destination = new Path(filePath + "_" + time)

        logger.info(s"File exists. Moving to destination: $destination")

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
    try {

      config.getConfig(s"$app")
    } catch {

      case exception: ConfigException => logger.error(s"Ivalid config key $app. Validate Config key provided")
        throw exception
    }
  }

  def getStructType(schema: String): StructType = {

    var struct = new StructType()

    try {

      schema
        .split(",")
        .foreach { column =>
          val field = column.split("-")
          struct = struct.add(field(0).trim, field(1))
        }

      struct.add("corrupt_record", StringType)
    } catch {
      case exception: ParseException => logger.error(s"Invalid schema string provided. Validate schema string from app.conf. Received $schema")
        throw exception
    }
  }

  def readCSV(app: String): DataFrame = {

    val conf = getConfObject(s"$app.csv")

    try {

      val a: String = conf.getString("schema")

      val structType: StructType = getStructType(a)

      spark.read
        .option("header", conf.getBoolean("header"))
        .option("delimiter", conf.getString("delimiter"))
        .option("mode", conf.getString("mode"))
        .option("multiLine", conf.getString("multiLine"))
        .schema(structType)
        .csv(conf.getString("file"))

    } catch {

      case exception: ConfigException => logger.error("Mentioned config is mandatory for read csv. Assign respective config in app.conf")
        throw exception

      case exception: IllegalArgumentException => logger.fatal("Invalid options given for csv config. Validate app.conf")
        throw exception

      case exception: AnalysisException => logger.error(s"Error reading file: ${conf.getString("file")} \n Please validate respective file's existence.")
        throw exception

    }
  }


  def writeParquet(app: String, dataKey: String, data: DataFrame): Unit = {

    val conf = getConfObject(s"$app.parquet")

    try {

      val path: String = conf.getString(dataKey)

      renameFile(path)
      data.write.parquet(path)
      logger.info(s"Written parquet at location $path")
      logger.info(s"$app - $dataKey - ${data.count()}")

    } catch {
      case exception: ConfigException => logger.error("Mentioned config is mandatory for read csv. Assign respective config in app.conf")
        throw exception

      case exception: Exception => logger.error(s"Error Writing file at location ${conf.getString(dataKey)}")
        throw exception
    }

  }

}
