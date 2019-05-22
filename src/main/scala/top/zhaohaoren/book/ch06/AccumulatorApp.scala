package top.zhaohaoren.book.ch06

import org.apache.spark.SparkContext

object AccumulatorApp extends App {

  def accumulatorBlankLine(sc: SparkContext): Unit = {
    val files = sc.textFile("./data.txt")
    val blankLines = sc.accumulator(0) // 创建Accumulator 并初始化为0

    val callSign = files.flatMap(line => {
      if (line == "") {
        blankLines += 1 // 累加器+1
      }
      line.split(" ")
    })

    callSign.saveAsTextFile("output.txt")
    println("Blank lines: " + blankLines.value)
  }
}
