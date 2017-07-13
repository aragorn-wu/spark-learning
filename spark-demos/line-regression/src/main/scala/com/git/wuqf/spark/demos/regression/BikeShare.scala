package com.git.wuqf.spark.demos.regression

import breeze.linalg.sum
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
  * Created by wuqf on 7/12/17.
  * line regression
  */
object BikeShare {
  def main(args: Array[String]) {
    lineRegression(initSparkContext(initLocalSparkConf()))
  }


  def initLocalSparkConf(): SparkConf = {
    val linuxPath = "/opt/bigdata/spark-2.1.1-bin-hadoop2.7";

    val conf = new SparkConf().setAppName("spark-bike").setMaster("local[*]").setSparkHome(linuxPath)
    return conf;
  }

  def initSparkContext(sparkConf: SparkConf): SparkContext = {
    var sc = new SparkContext(sparkConf)
    return sc
  }

  def lineRegression(sc: SparkContext): Unit = {
    val records = sc.textFile("src/main/resources/hour.csv").map(_.split(",")).cache()


    val mappings = for (i <- Range(2, 10)) yield splitDimension(records, i)

    val limittedVectorLength = sum(mappings.map(_.size))
    val unLimittedVectorLength = records.first().slice(10, 14).size
    val totalLength = limittedVectorLength + unLimittedVectorLength

    //产生训练数据集矩阵，按照前文所述处理类别特征和实数特征。
    val data = records.map { record =>

      val limittedVector = Array.ofDim[Double](limittedVectorLength)
      var i = 0
      var step = 0
      for (filed <- record.slice(2, 10)) {

        val m = mappings(i)
        val idx = m(filed)
        limittedVector(idx.toInt + step) = 1.0
        i = i + 1
        step = step + m.size
      }

      val unLimittedVector = record.slice(10, 14).map(x => x.toDouble)

      val features = limittedVector ++ unLimittedVector
      val label = record(record.size - 1).toInt


      LabeledPoint(label, Vectors.dense(features))
    }

    // val categoricalFeaturesInfo = Map[Int, Int]()
    //val linear_model=DecisionTree.trainRegressor(data,categoricalFeaturesInfo,"variance",5,32)

    val linear_model = LinearRegressionWithSGD.train(data, 40, 0.5)
    val true_vs_predicted = data.map(p => (p.label, linear_model.predict(p.features)))

    //输出前五个真实值与预测值
    println(true_vs_predicted.take(5).toVector.toString())
  }

  def splitDimension(rdd: RDD[Array[String]], idx: Int): Map[String, Long] = {
    //rdd.foreach(x=>println(x.length))
    rdd.map(field => field(idx)).distinct().zipWithIndex().collectAsMap().foreach(println)
    val md = rdd.map(filed => filed(idx)).distinct().zipWithIndex().collectAsMap()
    return md;
  }

}
