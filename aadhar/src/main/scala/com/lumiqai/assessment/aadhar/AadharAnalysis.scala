package com.lumiqai.assessment.aadhar

import com.lumiqai.assessment.utils.FilesUtil
import org.apache.spark.sql.types.{IntegerType, StructType}

object AadharAnalysis extends FilesUtil {

  def main(args: Array[String]): Unit = {

    val aadharData = readCSV("aadhar")

    val structTyped = new StructType()
      .add("date", IntegerType)

  }
}
