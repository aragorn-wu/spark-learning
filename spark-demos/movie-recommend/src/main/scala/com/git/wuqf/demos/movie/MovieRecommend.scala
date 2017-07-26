package com.git.wuqf.demos.movie

import org.apache.spark.{SparkConf, SparkContext}

object MovieRecommend {
  def main(args: Array[String]): Unit = {
    val conf = initLocalSparkConf()
    val sc = new SparkContext(conf)

  }

  def initLocalSparkConf(): SparkConf = {
    val linuxPath = "/opt/bigdata/spark-2.1.1-bin-hadoop2.7";

    val conf = new SparkConf().setAppName("movie-recommond").setMaster("local[*]").setSparkHome(linuxPath)
    return conf;
  }
}
