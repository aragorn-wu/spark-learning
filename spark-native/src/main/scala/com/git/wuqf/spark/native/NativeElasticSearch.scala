package com.git.wuqf.spark.native

import com.git.wuqf.spark.native.NativeFile.{initSparkContext, wordCounts}

/**
  * Created by Administrator on 2017/7/7.
  */
object NativeElasticSearch {
  def main(args: Array[String]): Unit = {
    wordCounts(initSparkContext())
  }

}
