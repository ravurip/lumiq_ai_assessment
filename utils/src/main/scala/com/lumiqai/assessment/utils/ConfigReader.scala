package com.lumiqai.assessment.utils

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger

trait ConfigReader {

  val logger: Logger = Logger.getLogger("lumiq.ai")
  val config: Config = {
    try {
      logger.info("Resolving application.conf")
      val confFile = new File(System.getProperty("confFile"))

      ConfigFactory.parseFile(confFile).resolve()

    } catch {

      case nullPointerException: NullPointerException => {
        logger.fatal("Application must be initialised with confFile")
        throw nullPointerException
      }
      case exception: Exception => {
        logger.error("Application terminated with following error in parsing config file")
        throw exception
      }
    }
  }

}
