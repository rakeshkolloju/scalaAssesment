package com.finance.gdp.batch

import com.finance.gdp.common.conf.SparkConfiguration
import org.apache.spark.sql.functions._

class ReadLatestPartition extends  SparkConfiguration {

  def getLatestPartition(filePath: String) : String = {
    val filePathRdd = spark.sparkContext.wholeTextFiles(filePath)
    val filePathRddKey = filePathRdd.keys

    import spark.implicits._

    val getFilePath = filePathRddKey
      .map( line => line.split("/")
      .dropRight(1)
      .mkString("/").replace("wasbs:", "abfss:").replace(".blob.", ".dfs."))
      .toDF("path")
    val latestPartitonPath = getFilePath
      .sort(desc("path"))
      .first()
      .getAs[String]("path")
    latestPartitonPath
  }

  def getFilesCount(filePath: String) : Long = {
    val filePathRdd = spark.sparkContext.wholeTextFiles(filePath)
    val fileKeysCount = filePathRdd.keys.count()
    fileKeysCount
  }
}