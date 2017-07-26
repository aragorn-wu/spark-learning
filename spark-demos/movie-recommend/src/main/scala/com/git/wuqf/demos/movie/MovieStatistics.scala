package com.git.wuqf.demos.movie

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object MovieStatistics {
  def main(args: Array[String]): Unit = {
    userStatistic(initSparkSession())
  }

  def userStatistic(sparkSession: SparkSession): Unit = {
    val userRDD = sparkSession.sparkContext.textFile("spark-demos/movie-recommend/src/main/resources/u.user");
    val schemaSring = "index age gender job zipcode"
    val fields = schemaSring.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRdd = userRDD.map(_.split("\\|")).map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4)))
    val userDF = sparkSession.createDataFrame(rowRdd, schema)
    userDF.createOrReplaceTempView("user")
    val resulults = sparkSession.sql("select age from user")
    resulults.show()
  }

  def initSparkSession(): SparkSession = {

    val conf = new SparkConf().setAppName("movie-statistic").setMaster("local[*]");

    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    return sparkSession;
  }
}
