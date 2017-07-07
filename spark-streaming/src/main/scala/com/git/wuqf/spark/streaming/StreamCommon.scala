package com.git.wuqf.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/7/7.
  */
object StreamCommon {

  def main(args: Array[String]): Unit = {
    blackListFilter(initSparkStreaming(initSparkConf()))
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

  def networkWordCount(ssc: StreamingContext): Unit = {
    val lines = ssc.socketTextStream("10.10.20.189", 8888)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.count().print()
    ssc.start()
    ssc.awaitTermination()
  }

  def blackListFilter(ssc: StreamingContext): Unit = {
    val bl = Array(("jim", true), ("hack", true))
    val blRDD = ssc.sparkContext.parallelize(bl, 4);
    val st = ssc.socketTextStream("localhost", 8888)
    val users = st.map { l =>
      (l.split(" ")(1), l)
    }
    val validRddDS = users.transform(ld => {
      val lt = ld.collect()
      val ljoinRdd = ld.leftOuterJoin(blRDD)
      val tmp = ljoinRdd.collect()
      val fRdd = ljoinRdd.filter(tuple => {
        if (tuple._2._2.getOrElse(false)) {
          false
        } else {
          true
        }
      })
      val ft = fRdd.collect();
      val validRdd = fRdd.map(tuple => {
        tuple._2._1
      })
      validRdd
    })

    validRddDS.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
