package com.git.wuqf.demos.movie

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object MovieStatistics {
  def main(args: Array[String]): Unit = {
    filmRating(initSparkSession())
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
      para.length match {
        case 3 => para(2)
        case _ => 1000
      }
    })

    val yearsCount = years.countByValue();
    yearsCount.keys.foreach(i =>
      println("year is :" + i + ". count is :" + yearsCount(i))
    )
  }

  def filmRating(sparkSession: SparkSession): Unit = {
    val ratingData = sparkSession.sparkContext.textFile("spark-demos/movie-recommend/src/main/resources/u.data");
    println(ratingData.first())
    println(ratingData.count())
    val count = ratingData.count();

    val sratings = ratingData.map(line => line.split("\t")(2))
    val doubleRatings = sratings.map(x => x.toDouble);

    println(doubleRatings.max())

    val ratingCount = doubleRatings.countByValue();
    ratingCount.foreach(println)

    val meanRating = doubleRatings.reduce((x, y) =>
      x + y
    ) / count;
    println(meanRating)

    val ratingTimesPerUser = ratingData.map(line => line.split("\t")(0)).countByValue()
    ratingTimesPerUser.foreach(println)
  }

}
