package com.git.wuqf.demos.movie

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object MovieStatistics {
  def main(args: Array[String]): Unit = {
    filmConvertYear(initSparkSession())
  }

  def userStatistic(sparkSession: SparkSession): Unit = {
    val userRDD = sparkSession.sparkContext.textFile("spark-demos/movie-recommend/src/main/resources/u.user");
    val schemaSring = "index age gender job zipcode"
    val fields = schemaSring.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRdd = userRDD.map(_.split("\\|")).map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4)))
    val userDF = sparkSession.createDataFrame(rowRdd, schema)
    userDF.createOrReplaceTempView("user")
    val resulults = sparkSession.sql(SqlConstant.userSql.ageStatistic)
    resulults.show()
  }

  def initSparkSession(): SparkSession = {

    val conf = new SparkConf().setAppName("movie-statistic").setMaster("local[*]");

    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    return sparkSession;
  }

  def filmConvertYear(sparkSession: SparkSession): Unit = {
    val lines = sparkSession.sparkContext.textFile("spark-demos/movie-recommend/src/main/resources/u.item");

    val years = lines.map(line => line.split("\\|")(2)).map(line => {
      val para = line.split("-")
        try {
          para.length match {
            case 3 => para(2)
            case _ => 1000
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }

    })
    years.foreach(println(_))
  }

}
