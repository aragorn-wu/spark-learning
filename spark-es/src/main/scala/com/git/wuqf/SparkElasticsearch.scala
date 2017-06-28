package com.git.wuqf

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by Administrator on 2017/6/20.
  */
object SparkElasticsearch {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark-es").setMaster("spark://10.10.20.189:7077")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "10.10.20.184")
      .setJars(List("D:\\git\\spark-demo\\spark-es\\target\\spark-es-1.0-SNAPSHOT.jar","C:\\Users\\Administrator\\.m2\\repository\\org\\elasticsearch\\elasticsearch-hadoop\\5.4.2\\elasticsearch-hadoop-5.4.2.jar"));

    var sc = new SparkContext(conf);

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
    val rdd = sc.makeRDD(Seq(numbers, airports))

    EsSpark.saveToEs(rdd, "spark/docs");

  }


}
