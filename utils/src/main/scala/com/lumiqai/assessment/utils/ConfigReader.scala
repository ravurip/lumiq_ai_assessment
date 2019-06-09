package com.lumiqai.assessment.utils

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigException}
import org.apache.log4j.Logger

trait ConfigReader {

  val logger: Logger = Logger.getLogger("lumiq.ai")
  val config: Config = {
    try {
      logger.info("Initializing Application. Resolving application.conf")

      val confFile = new File(System.getProperty("confFile"))
      val conf = ConfigFactory.parseFile(confFile).resolve()

      logger.info("Successfully resolved app.conf")
      conf
    } catch {

      case nullPointerException: NullPointerException =>
        logger.fatal("Application must be initialised with confFile")
        throw nullPointerException

      case exception: ConfigException =>
        logger.error("Failed parsing app.conf. validate app.conf")
        throw exception

      case exception: Exception => logger.error("Error in resolving conf file")
        throw exception
    }
  }
}
