package com.git.wuqf.spark.sql

import org.apache.spark.sql.SparkSession


/**
  * Created by Administrator on 2017/6/20.
  */
object SqlMysql {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-mysql")
      .master("spark://10.10.20.189:7077")
      .getOrCreate();
    spark.sparkContext.addJar("C:\\Users\\Administrator\\.m2\\repository\\mysql\\mysql-connector-java\\5.1.30\\mysql-connector-java-5.1.30.jar");


    val df = spark.read.jdbc("jdbc:mysql://10.10.20.183:3306/mysql?user=root&password=root","db",null);

    df.printSchema
    df.select("h_data").show(10)

    df.createOrReplaceTempView("rs")
    var sqlDF = spark.sql("select * from rs limit 10");
    sqlDF.show();
  }


}
