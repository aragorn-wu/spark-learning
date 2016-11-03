package com.git.wuqf

import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Hello world!
  *
  */
object mysql {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark-mysql").master("spark://hadoop01:7077").getOrCreate();

    spark.sparkContext.addJar("C:\\Users\\sdzn-dsj\\.m2\\repository\\mysql\\mysql-connector-java\\5.1.40\\mysql-connector-java-5.1.40.jar");

    val df = spark.sqlContext.load("jdbc", Map("url" -> "jdbc:mysql://192.168.0.118:3306/resource?user=root&password=sdzn123456", "dbtable" -> "file_resource"));
    df.printSchema();
    df.select("resource_name").show(10);

    df.createOrReplaceTempView("rs")
    var sqlDF=spark.sql("select * from rs limit 10");
    sqlDF.show();
  }
}
