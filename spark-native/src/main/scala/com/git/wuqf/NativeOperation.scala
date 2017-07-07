package com.git.wuqf

//import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.immutable.HashMap


/**
  * Created by Administrator on 2017/6/20.
  */
object NativeOperation {

  //Logger.getLogger("org").setLevel(Level.DEBUG)

  def main(args: Array[String]): Unit = {
    wordCounts(initSparkContext())
  }

  def initSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("textCount").setMaster("spark://10.10.20.189:7077")
      .setJars(List("D:\\git\\spark-demo\\spark-file\\target\\spark-file-1.0-SNAPSHOT.jar"));
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
