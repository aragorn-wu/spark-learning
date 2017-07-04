package com.git.wuqf

//import org.apache.log4j.{Level, Logger}
import java.util

import org.apache.hadoop.io.{MapWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.immutable.HashMap


/**
  * Created by Administrator on 2017/6/20.
  */
object SparkElasticsearch {

  //Logger.getLogger("org").setLevel(Level.DEBUG)

  def main(args: Array[String]): Unit = {
    var sc = initSpark();
    ipCount(sc)
    utilSave(sc)
  }

  def utilSave(sc: SparkContext): Unit = {
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val rdd = sc.makeRDD(Seq(numbers))
    EsSpark.saveToEs(rdd, "spark/docs")
  }

  def directSave(sc: SparkContext): Unit = {
    val numbers = Map("one" -> 10, "two" -> 20, "three" -> 30)
    val airports = Map("arrival" -> "Otopeni1", "SFO" -> "San Fran1")
    sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")
  }

  def initSpark(): SparkContext = {
    val conf = new SparkConf().setAppName("spark-es").setMaster("spark://10.10.20.189:7077")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "10.10.20.189")
      .setJars(List("D:\\git\\spark-demo\\spark-es\\target\\spark-es-1.0-SNAPSHOT.jar",
        "C:\\Users\\Administrator\\.m2\\repository\\org\\elasticsearch\\elasticsearch-spark-20_2.11\\5.4.4\\elasticsearch-spark-20_2.11-5.4.4.jar"));

    var sc = new SparkContext(conf);
    return sc;
  }

  def getCount(sc: SparkContext): Unit = {
    val rdd = EsSpark.esRDD(sc, "apache-access-2016/apache-access")
    val count = rdd.count()
    print(count + "\n")
  }


  def ipCount(sc: SparkContext): Unit = {
    val rdd = EsSpark.esRDD(sc, "apache-access-2016/apache-access")

    var ipCounts: Map[String, Int] = new HashMap[String, Int]()
    val datas = rdd.take(10);
    datas.foreach { content =>
      val ip = String.valueOf(content._2("clientip"))

      if (!ipCounts.contains(ip)) {
        ipCounts = ipCounts.updated(ip, 1)
      } else {
        ipCounts = ipCounts.updated(ip, ipCounts(ip) + 1)
      }
    }

    ipCounts.keys.foreach(i => println("key:" + i + ". value:" + ipCounts(i)))
    println("xxxxxxxxxxxxxxxx")
  }

}
