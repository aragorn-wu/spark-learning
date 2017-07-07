package com.git.wuqf.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/7/7.
  */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {
    wordCount(initSparkStreaming(initSparkConf()))
  }

  def initSparkConf(): SparkConf = {
    val conf = new SparkConf().setAppName("spark-streaming-flume").setMaster("spark://10.10.20.189:7077")

      .setJars(List("C:\\Users\\Administrator\\.m2\\repository\\org\\elasticsearch\\elasticsearch-spark-20_2.11\\5.4.4\\elasticsearch-spark-20_2.11-5.4.4.jar",
        "D:\\git\\spark-demo\\spark-streaming\\target\\spark-streaming-1.0-SNAPSHOT.jar"));

    return conf;
  }

  def initSparkStreaming(sparkConf: SparkConf): StreamingContext = {
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    return ssc;
  }

  def wordCount(ssc:StreamingContext): Unit = {
    val lines=ssc.socketTextStream("10.10.20.189",1111)
    val words=lines.flatMap(_.split(" "))
    val pairs=words.map(word=>(word,1))
    val wordCounts=pairs.reduceByKey(_+_)
    wordCounts.count().print()
    ssc.start()
    ssc.awaitTermination()
  }
}
