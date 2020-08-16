package com.finance.gdp.common.conf

import org.apache.spark.sql.SparkSession

class SparkConfiguration() {

  val spark: SparkSession = SparkSession.builder().
    config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .appName("Gdp_Finance_Batch").getOrCreate()
}