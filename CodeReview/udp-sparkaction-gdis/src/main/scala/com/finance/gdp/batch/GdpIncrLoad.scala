package com.finance.gdp.batch

import java.util.Properties

import com.finance.gdp.common.conf.{InitAdls, ReadConfiguration, SparkConfiguration}
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, Row}

class GdpIncrLoad (inputAdlsFolder: String, outputAdlsFolder: String, typeOfInputFile: String,
                  typeOfOutputFile: String, primaryKey: String) extends SparkConfiguration {
try {
  new InitAdls()

  val propPath: Properties = new ReadConfiguration().configurationProperties("/config/application.config")
  val masterContainer: String = propPath.getProperty("masterContainer")
  val standardContainer: String = propPath.getProperty("standardContainer")

  val standardLatestPartition: String = new ReadLatestPartition().getLatestPartition(standardContainer +
    inputAdlsFolder)
  val masterLatestPartition: String = new ReadLatestPartition().getLatestPartition(masterContainer +
    outputAdlsFolder)

  if (typeOfInputFile == "orc") {
    val masterLoadData: DataFrame = spark.read.orc(masterLatestPartition)
    val standardLoadData: DataFrame = spark.read.orc(standardLatestPartition)
    masterLoadData.createOrReplaceTempView("masterLoadData")
    standardLoadData.createOrReplaceTempView("standardLoadData")
    val splitPrimaryKey = primaryKey.split(",")
    var queryFinal = ""
    for (x <- splitPrimaryKey) {
      queryFinal += "standardLoadData." + x + " = masterLoadData." + x + " and "
    }
    queryFinal = queryFinal.slice(0, queryFinal.length - 4)
    val joinDf = spark.sql("SELECT masterLoadData.* FROM masterLoadData " +
      "LEFT ANTI JOIN standardLoadData ON" + queryFinal)
    val finalMasterDF = standardLoadData.union(joinDf)
    new GdpMetaDataLoad(standardLatestPartition, finalMasterDF, outputAdlsFolder, typeOfOutputFile)
     }
   }
   catch {
       // scalastyle:off println
       case ae: AnalysisException =>
       println("Exception Message: " + ae.getMessage)
       case unknown: Exception =>
       println("Exception Message: " + unknown.getMessage)
     }
}