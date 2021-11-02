package com.shape.test.data.config

object environment extends Enumeration {

  val conf = com.typesafe.config.ConfigFactory.load()

  def getSourceLogsSource(isLocal: Boolean): String = {
    val sourceKey = if (isLocal) "Local" else "Remote"
    val configValue = s"Configuration.$sourceKey.SourceLogsSource"
    conf.getString(configValue)
  }

  def getSourceJsonSource(isLocal: Boolean): String = {
    val sourceKey = if (isLocal) "Local" else "Remote"
    val configValue = s"Configuration.$sourceKey.SourceJsonSource"
    conf.getString(configValue)
  }

  def getSourceCsvSource(isLocal: Boolean): String = {
    val sourceKey = if (isLocal) "Local" else "Remote"
    val configValue = s"Configuration.$sourceKey.SourceCsvSource"
    conf.getString(configValue)
  }

  def DataLakeLocation(isLocal: Boolean): String = {
    val sourceKey = if (isLocal) "Local" else "Remote"
    val configValue = s"Configuration.$sourceKey.DataLakeLocation"
    conf.getString(configValue)
  }

  def isRunningLocalMode(): Boolean = {
    val isLocalMode = conf.getBoolean("Configuration.Setup.Running_Local_Mode")

    if(isLocalMode) return isLocalMode else throw new NotImplementedError("This solution for while only running in local mode. Please set Running_Local_Mode=true in config file.")
  }
}
