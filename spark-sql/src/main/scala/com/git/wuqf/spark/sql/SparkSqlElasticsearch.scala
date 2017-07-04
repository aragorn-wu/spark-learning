package com.git.wuqf.spark.sql

//import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark


/**
  * Created by Administrator on 2017/6/20.
  */
object SparkSqlElasticsearch {

  //Logger.getLogger("org").setLevel(Level.DEBUG)

  def main(args: Array[String]): Unit = {
    var sc = initSpark();
    ipCount(initSparkSession(initSpark()))
  }

  def initSpark(): SparkConf = {
    val conf = new SparkConf().setAppName("spark-sql-es").setMaster("spark://10.10.20.189:7077")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "10.10.20.189")
      .setJars(List("C:\\Users\\Administrator\\.m2\\repository\\org\\elasticsearch\\elasticsearch-spark-20_2.11\\5.4.4\\elasticsearch-spark-20_2.11-5.4.4.jar",
        "D:\\git\\spark-demo\\spark-sql\\target\\spark-sql-1.0-SNAPSHOT.jar"));

    return conf;
  }

  def initSparkSession(conf: SparkConf): SparkSession = {
    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    return sparkSession;
  }

  def ipCount(sc: SparkSession): Unit = {
    val options = Map("pushdown" -> "true")
    val access = sc.read.format("org.elasticsearch.spark.sql")
      .options(options)
      .load("apache-access-2016/apache-access")
    val ds = access
      .select("clientip")
      .groupBy("clientip")
      .count()
      .rdd.map(row => Map("accessIp" -> row.getString(0), "accessTimes" -> row.getLong(1)))
    EsSpark.saveToEs(ds, "apache-acess/access-count")
  }

}
