package com.git.wuqf.decesiontree


import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wuqf on 7/14/17.
  */
object BoyFriend {
  def main(args: Array[String]): Unit = {
    val sparkConf = initLocalSparkConf()
    val sc = new SparkContext(sparkConf)
    val trainSource = sc.textFile("boyfriend/train.dat")
    val trainTree = getDataTree(trainSource)

    val testSource = sc.textFile("boyfriend/test.dat")
    val testTree = getDataTree(testSource)

    val (trainData, testData) = (trainTree, testTree)
    val numberClass = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32
    val model = DecisionTree.trainClassifier(trainData, numberClass, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val print_predict = labelAndPreds.take(5)
    for (i <- 0 to 4) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("test Error"+testErr)
    println("Learned classification tree model:\n" + model.toDebugString)

  }

  def initLocalSparkConf(): SparkConf = {
    val linuxPath = "/opt/bigdata/spark-2.1.1-bin-hadoop2.7";

    val conf = new SparkConf().setAppName("boyfriend").setMaster("local[*]").setSparkHome(linuxPath)
    return conf;
  }

  def getDataTree(sourceData: RDD[String]): RDD[LabeledPoint] = {
    val tree = sourceData.map { line =>
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }
    return tree
  }
}
