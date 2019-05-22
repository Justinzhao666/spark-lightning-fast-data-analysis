package top.zhaohaoren.book.ch03

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object RDDApp extends App {

  def createRDD(sc: SparkContext): Unit = {
    // 最简单的创建RDD，这个会自动创建分区 试了好几个分区数量都为2
    val lines = sc.parallelize(Array[String]("justin", "lucy", "jack"))
    // 外部数据集创建RDD
    val lines2 = sc.textFile("./file")
  }

  def operateRDD(inputRDD: RDD[String]): Unit = {
    /// 转化操作
    // filter
    val errorRDD = inputRDD.filter(item => item.contains("error"))
    val warnRDD = inputRDD.filter(item => item.contains("warn"))
    // map
    val intRDD = inputRDD.map(item => item.length)
    // flatMap
    val wordsRDD = inputRDD.flatMap(item => item.split(" "))
    // 集合操作
    // 交
    val badRDD = errorRDD.union(warnRDD)
    // 去重
    wordsRDD.distinct() // 开销很大，所有集群数据网络传输进行混洗
    // 交并去重
    errorRDD.intersection(warnRDD) // 性能差，数据混洗
    // 差
    errorRDD.subtract(warnRDD) // error有 warn没有的 error-warn
    // 笛卡尔积
    errorRDD.cartesian(warnRDD) // 大数据时候开销很大

    /// 行动操作
    // reduce
    intRDD.reduce((x, y) => x + y) // 求和
    // fold rdd的fold和scala的还有些不同
    /*
     * 计算过程：
     * val lines = sc.parallelize(Array(1,2))
     * lines.fold(10)((x,y) => {println(x+ "\t" +y);    x+y})
     * parallelize 会有一个默认的分区 大多是2 lines.partitions.size 可以获取到分区的个数
     * 说明分了两个区:
      * 1）：1
      * 2）：2
      * 然后针对两个区分别求 res1:zeroValue+1,res2:zeroValue+2
      * 最后zeroValue+res1+res2 每一个区之间再进行运算
      * 结果为 10+1 10+2 + 10
     */
    intRDD.fold(0)((x, y) => x + y) // 对于运算给一个初值，这个初值应该是不影响多次运算结果的，如： + 用0 * 用1


    println(badRDD.count())
    badRDD.take(10).foreach(println)
    println(badRDD.collect()) //大数据情况下不可以使用。数据量必须单机内存能撑得起的情形才能用；性能差，数据混洗
  }

  def persist(inputRDD: RDD[Int]) = {
    val result = inputRDD.map(x => x * x)
    result.persist(StorageLevel.DISK_ONLY) // 设置缓存和持久化级别
    // 下面要算两次所以应该缓存下
    println(result.count())
    println(result.collect().mkString(","))
    inputRDD.unpersist() // 手动把持久化的RDD从缓存中移除。
  }

}

class SearchFunction(val query: String) {
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatchesFunctionReference(rdd: RDD[String]): RDD[String] = {
    // 不安全： isMatch 是 this.isMatch，会传递整个this对象给计算节点
    rdd.filter(isMatch)
  }

  def getMatchesFieldReference(rdd: RDD[String]): RDD[Array[String]] = {
    // 不安全：query 是被该类引用的，所以会传递整个this
    rdd.map(x => x.split(query))
  }

  def getMatchesNoReference(rdd: RDD[String]) = {
    // 安全的，传递的是不带引用的字段（将某个类的对象的某个字段或者函数放到局部变量中去）
    val query_ = this.query
    rdd.map(x => x.split(query_))
  }
}
