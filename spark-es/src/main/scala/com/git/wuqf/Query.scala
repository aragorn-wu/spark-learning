package com.git.wuqf

import com.git.wuqf.Save.{directSave, initSpark}
import org.apache.spark.SparkContext
import org.elasticsearch.spark._

/**
  * Created by Administrator on 2017/6/28.
  */
object Query {
  def main(args: Array[String]): Unit = {
    var sc = initSpark();
    queryAll(sc);
  }

  def queryAll(sc: SparkContext): Unit = {
    val RDD = sc.esRDD("spark/docs")

  }
}
