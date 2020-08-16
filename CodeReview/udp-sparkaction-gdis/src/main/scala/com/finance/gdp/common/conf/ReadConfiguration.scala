package com.finance.gdp.common.conf

import java.util.Properties
class ReadConfiguration {
  def configurationProperties(path: String) : Properties =
  {
    val props: Properties = new Properties()
    val filepath = getClass.getResourceAsStream(path)
    props.load(filepath)
    props
  }
}
