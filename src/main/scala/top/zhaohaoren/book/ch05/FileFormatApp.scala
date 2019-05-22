package top.zhaohaoren.book.ch05

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.SparkContext

/**
  * 文件格式 读取和保存
  */
object FileFormatApp extends App {

  case class Person(name: String, lovesPandas: Boolean) // Note: must be a top level class

  def TextFile(sc: SparkContext): Unit = {
    /** 读取文件 */
    // 如果是一个文件，直接读取到RDD
    val inputs = sc.textFile("xx.txt")
    // 如果是个目录，该目录下的文件都读到RDD
    val inputsPath = sc.textFile("/path/")
    // 如果按照文件名区分文件来源，使用PairRDD 可以获取文件名key的
    val inputsPair = sc.wholeTextFiles("/path/")
    // 计算平均值
    val result = inputsPair.mapValues { y =>
      val nums = y.split(" ").map(x => x.toDouble)
      nums.sum / nums.size.toDouble
    }

    /** 保存文件 */
    // 保存文件
    result.saveAsTextFile("/result.txt")
  }

  def JSONFile(sc: SparkContext): Unit = {
    /** 读取JSON */
    val inputs = sc.textFile("xx.json")
    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    val res = inputs.flatMap(record => {
      try {
        Some(mapper.readValue(record, classOf[Person]))
      } catch {
        case _: Exception => None
      }
    })

    /** 保存JSON */
    res.filter(r => r.lovesPandas).map(mapper.writeValueAsString(_)).saveAsTextFile("/result.txt")
  }

  def CSVAndTSVFile(sc: SparkContext): Unit = {
    /** 读取 */
    // 略
    /** 保存 */
    // 略
  }

  def SequenceFile(sc: SparkContext): Unit = {
    /** 读取SequenceFile */
    val inputFile = "/a.txt"
    val data = sc.sequenceFile(inputFile, classOf[Text], classOf[IntWritable])
      .map { case (x, y) => (x.toString, y.get()) }

    /** 保存SequenceFile */
    data.saveAsSequenceFile("/result.txt")
  }

  def ObjectFile(sc: SparkContext): Unit = {
    /** 读取 */
    // 略
    /** 保存 */
    // 略
  }

  def HadoopFile(sc: SparkContext): Unit = {
    /** 读取 */
    // 略
    /** 保存 */
    // 略
  }



}
