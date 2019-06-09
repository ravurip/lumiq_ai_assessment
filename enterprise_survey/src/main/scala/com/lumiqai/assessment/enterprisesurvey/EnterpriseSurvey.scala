package com.lumiqai.assessment.enterprisesurvey

import org.apache.spark.sql.functions.col
import com.lumiqai.assessment.utils.FilesUtil

object EnterpriseSurvey extends FilesUtil {

  def main(args: Array[String]): Unit = {

    val df = readCSV("enterpriseSurvey")

    df.printSchema()

    df.filter(col("corrupt_record").isNotNull).show(false)

  }

}