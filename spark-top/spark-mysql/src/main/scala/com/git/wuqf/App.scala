package com.git.wuqf

import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Hello world!
  *
  */
object App {

  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("spark-mysql").master("spark://hadoop01:7077").getOrCreate();
    val jdbcDF = spark.sqlContext.load("jdbc", Map("url" -> "jdbc:mysql://192.168.0.118:3306/resource?user=root&password=sdzn123456", "dbtable" -> "file_resource"))
    jdbcDF.printSchema();

  }
}
