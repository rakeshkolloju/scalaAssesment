package com.finance.gdp.batch

import com.finance.gdp.common.conf.SparkConfiguration

object GdpSparkAction  extends  SparkConfiguration {

  def main(args: Array[String]): Unit = {
    try {
      val typeOfLoad = args(0)
      val inputAdlsFolder = args(1)
      val outputAdlsFolder = args(2)
      val typeOfInputFile = args(3)
      val typeOfOutputFile = args(4)
      val primaryKey = args(5)

      if (typeOfLoad == "full") {
        new GdpBatchFullLoad(inputAdlsFolder, outputAdlsFolder, typeOfInputFile, typeOfOutputFile)
      }
      else if (typeOfLoad == "increment") {
        new GdpIncrLoad(inputAdlsFolder, outputAdlsFolder, typeOfInputFile, typeOfOutputFile, primaryKey)
      }
      else {
        print("Invalid Argument provided ")
      }
    }
    catch {
      // scalastyle:off println
      case ae: ArrayIndexOutOfBoundsException =>
        println("Exception Message: " + ae.getMessage)
      case unknown: Exception =>
        println("Exception Message: " + unknown.getMessage)
    }
    finally {
      spark.stop()
    }
  }
}