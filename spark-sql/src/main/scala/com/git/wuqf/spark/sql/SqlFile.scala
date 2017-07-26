package com.git.wuqf.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Administrator on 2017/7/7.
  */
object SqlFile {

  def main(args: Array[String]): Unit = {
    testSchema(initSparkSession())
  }

  def testSchema(sparkSession: SparkSession): Unit = {
    val peopleRDD = sparkSession.sparkContext.textFile("spark-sql/src/main/resources/people.txt")
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

  def initSparkSession(): SparkSession = {

    val conf = new SparkConf().setAppName("spark-sql-file").setMaster("local[*]")

    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    return sparkSession;
  }
}
