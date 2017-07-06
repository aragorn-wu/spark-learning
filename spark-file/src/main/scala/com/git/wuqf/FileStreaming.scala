package com.git.wuqf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/20.
  */
object FileStreaming {
  def main(args: Array[String]): Unit = {
    testSchema(initSparkSession(initSparkConf()))
  }

  def initSparkConf(): SparkConf = {
    val conf = new SparkConf().setAppName("textCount").setMaster("spark://10.10.20.189:7077")
      .setJars(List("D:\\git\\spark-demo\\spark-file\\target\\spark-file-1.0-SNAPSHOT.jar"));
    return conf
  }

  def initSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("textCount").setMaster("spark://10.10.20.189:7077")
      .setJars(List("D:\\git\\spark-demo\\spark-file\\target\\spark-file-1.0-SNAPSHOT.jar"));
    val sc = new SparkContext(conf)
    return sc
  }

  def initSparkSession(conf: SparkConf): SparkSession = {
    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    return sparkSession;
  }

  def wordCounts(sc: SparkContext): Unit = {
    val lines = sc.textFile("data.txt");
    val lineLengths = lines.map(s => s.length);
    val totalLength = lineLengths.reduce((a, b) => a + b);

    println(totalLength)
  }

  def testClosures(sc: SparkContext): Unit = {
    val data = Array(1, 2, 3, 4, 5)
    var counter = 0;
    var rdd = sc.parallelize(data)
    rdd.foreach(x => counter + x)
    println("Counter value: " + counter)
  }

  def testSchema(sparkSession: SparkSession): Unit = {
    val peopleRDD = sparkSession.sparkContext.textFile("people.txt")
    val schemaString = "name age"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))
    val peopleDF = sparkSession.createDataFrame(rowRDD, schema)
    peopleDF.createOrReplaceTempView("people")
    val results = sparkSession.sql("select name from people")
    results.show()
  }

}
