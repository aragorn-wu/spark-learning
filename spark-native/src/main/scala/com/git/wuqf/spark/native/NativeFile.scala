package com.git.wuqf.spark.native

//import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/20.
  */
object NativeFile {

  //Logger.getLogger("org").setLevel(Level.DEBUG)

  def main(args: Array[String]): Unit = {
    wordCounts(initSparkContext())
  }

  def initSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("textCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    return sc
  }

  def wordCounts(sc: SparkContext): Unit = {
    val lines = sc.textFile("data.txt");

    val lineLengths = lines.map(s => s.length);
    val totalLength = lineLengths.reduce((a, b) => a + b);
    lineLengths.foreach(println)
    println(totalLength)
  }
}
