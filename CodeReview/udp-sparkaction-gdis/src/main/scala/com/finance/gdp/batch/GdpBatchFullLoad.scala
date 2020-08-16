package com.finance.gdp.batch

import java.util.Properties

import com.finance.gdp.common.conf.{InitAdls, ReadConfiguration, SparkConfiguration}
import org.apache.spark.sql.{AnalysisException, DataFrame}

class GdpBatchFullLoad(inputAdlsFolder: String, outputAdlsFolder: String, typeOfInputFile: String,
                        typeOfOutputFile: String) extends SparkConfiguration {
  try {
    new InitAdls()
    val readConfig: Properties = new ReadConfiguration().configurationProperties("/config/application.config")
    val standardContainer: String = readConfig.getProperty("standardContainer")
    val maxPartitionPath: String = new ReadLatestPartition().getLatestPartition(standardContainer + inputAdlsFolder)

    if (typeOfInputFile == "orc") {
      val loadData: DataFrame = spark.read.orc(maxPartitionPath)
      new GdpMetaDataLoad( maxPartitionPath, loadData, outputAdlsFolder, typeOfOutputFile)
    }
  }
  catch {
    // scalastyle:off println
    case ae: AnalysisException =>
      println("Exception Message: " + ae.getMessage)
      spark.stop()
    case unknown: Exception =>
      println("Exception Message: " + unknown.getMessage)
      spark.stop()
  }
}
