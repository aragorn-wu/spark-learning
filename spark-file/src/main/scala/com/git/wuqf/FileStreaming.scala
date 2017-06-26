package com.git.wuqf

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/20.
  */
object FileStreaming {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("textCount").setMaster("spark://10.10.20.189:7077")
      .setJars(List("D:\\git\\spark-demo\\spark-file\\target\\spark-file-1.0-SNAPSHOT.jar"));
    var sc = new SparkContext(conf);
    var path="spark-file\\src\\main\\resources\\data.txt";

    wordCounts(sc,path);
  }

  def wordCounts(sc:SparkContext,path:String):Unit={
    val lines = sc.textFile(path);
    val lineLengths = lines.map(s => s.length);
    val totalLength = lineLengths.reduce((a, b) => a + b);
  }

}
