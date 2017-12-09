package com.semrush

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Object is intended to save shared data for all tests e.g. SparkContext etc.
  */
object TestSuite {

  val conf: SparkConf = new SparkConf().setAppName("scalyzer_test_suite").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)

}
