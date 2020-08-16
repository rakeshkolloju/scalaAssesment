package com.finance.gdp.common.conf

import java.io.IOException
import java.util.Properties

import org.apache.spark.sql.AnalysisException

class InitAdls extends SparkConfiguration {
  try {
    val readConfig: Properties = new ReadConfiguration().configurationProperties("/config/application.config")
    val AccountKey: String = readConfig.getProperty("key")
    val storageAccountName: String = readConfig.getProperty("storageAccountName")

    val container: String = readConfig.getProperty("containerName")
    spark.sparkContext.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key." + storageAccountName + ".blob.core.windows.net", AccountKey)

    spark.conf.set("fs.defaultFS", "abfss://" + container + "@" + storageAccountName + ".dfs.core.windows.net")
    spark.conf.set("fs.azure.account.key." + storageAccountName + ".dfs.core.windows.net", AccountKey)

   /* spark.conf.set("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb")
    spark.conf.set("fs.AbstractFileSystem.wasbs.impl", "org.apache.hadoop.fs.azure.Wasbs")
    spark.conf.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    spark.conf.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure") */

    spark.conf.set("fs.azure.secure.mode", false)
    spark.conf.set("fs.abfs.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem")
    spark.conf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem")
    spark.conf.set("fs.AbstractFileSystem.abfs.impl", "org.apache.hadoop.fs.azurebfs.Abfs")
    spark.conf.set("fs.AbstractFileSystem.abfss.impl", "org.apache.hadoop.fs.azurebfs.Abfss")
    spark.conf.set("fs.azure.local.sas.key.mode", false)
  }
  catch {
    // scalastyle:off println
    case ae: AnalysisException =>
      println("Exception Message: " + ae.getMessage)
      spark.stop
    case io: IOException =>
      println("Exception Message : " + io.getMessage)
      spark.stop
    case unknown: Exception =>
      println("Exception Message: " + unknown.getMessage)
      spark.stop
  }
  }
