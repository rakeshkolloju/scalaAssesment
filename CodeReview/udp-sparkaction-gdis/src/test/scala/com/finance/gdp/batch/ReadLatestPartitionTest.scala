package com.finance.gdp.batch

import com.finance.gdp.common.conf.{InitAdls, SparkConfiguration}
import org.scalatest.FunSuite

class ReadLatestPartitionTest extends FunSuite{
  new SparkConfiguration()
  new InitAdls()
  val latestpartition = new  ReadLatestPartition()
  test("ReadLatestPartitions.getLatestPartition") {
    val filePath = "abfss://af203jd@aksdf2309i.core.windows.net/in/masterreferencedata/sap/financeorgxref/*/*/*/"
    val output ="abfss://m1209hjb9b34bu@0039fniub13uoij.dfs.core.windows.net/in/masterreferencedata/sap/financeorgxref/2020/07/24"
    assert(latestpartition.getLatestPartition(filePath) === output)
  }
  test("ReadLatestPartitions.checkPartition") {
    val output ="abfss://039fin2byiuh@0iubih4ghmlnnbedvq.dfs.core.windows.net/in/masterreferencedata/sap/financeorgxref/*/*/*/"
    assert(latestpartition.getFilesCount(output) === 1)
  }
}

