package top.zhaohaoren.book.ch02

import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark 计算work count
  */
object WordCountApp extends App {
  // 第一步：创建SparkConf配置我们的应用信息
  val conf = new SparkConf().setMaster("node1").setAppName("myApp")
  // 通过配置创建SparkContext对象
  val sc = new SparkContext(conf)

  //RDD操作 wordCount
  val inputFile = args(0)
  val outputFile = args(1)
  val input = sc.textFile(inputFile)
  val words = input.flatMap(line => line.split(" "))
  val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
  counts.saveAsTextFile(outputFile)

  // 关闭spark
  sc.stop() // 或者可以直接System.exit(0)退出程序

}
