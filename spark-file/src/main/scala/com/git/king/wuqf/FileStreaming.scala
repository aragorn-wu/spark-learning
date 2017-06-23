package com.git.king.wuqf

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * Created by Administrator on 2017/6/20.
  */
object FileStreaming {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("textCount").setMaster("spark://king-PowerEdge-R410:7077")
      .setJars(List("D:\\git\\spark-demo\\spark-file\\target\\spark-file-1.0-SNAPSHOT.jar"));
    var sc=new SparkContext(conf);
    val lines = sc.textFile("data.txt")
    val lineLengths = lines.map(s => s.length)
    val totalLength = lineLengths.reduce((a, b) => a + b)

  }
}
