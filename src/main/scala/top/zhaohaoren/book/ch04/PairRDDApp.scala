package top.zhaohaoren.book.ch04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PairRDDApp extends App {
  val conf = new SparkConf().setMaster("node1").setAppName("myApp")
  val sc = new SparkContext(conf)
  val lines = sc.parallelize(List("a b", "b c", "a c"))
  // 普通RDD转键值对RDD
  val pairs = lines.map(x => (x.split(" ")(0), x))

  def transPairRDD(pairs: RDD[(String, String)]) = {
    // mapValue相当于map，只是针对kv类型封装了下将函数作用于v
    // reduceByKey 是一个转化操作，不是一个action操作，返回的RDD[(K, V)]
    pairs.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    // word count的案例就是这种方式
  }
}
