package top.zhaohaoren.book.ch04

import org.apache.spark.{HashPartitioner, SparkContext}

class PageRankApp extends App {

  def pageRank(sc: SparkContext): Unit = {

    // links 存放pageId 和 该page相邻页面的列表
    val links = sc.objectFile[(String, Seq[String])]("link")
      .partitionBy(new HashPartitioner(100))
      .persist()

    // ranks 存放pageId和 该page分值, 初始状态想先全部都设置为1
    var ranks = links.mapValues(_ => 1.0)

    for (i <- 1.until(10)) {
      val contributions = links.join(ranks).flatMap {
        // 为pageId的相邻每个page添加一个值：当前page的Rank/当前page的size
        case (pageId, (link, rank)) => link.map(dest => (dest, rank / link.size))
        // 此时dest为关联page的Id，值为该关联page在其主page关联下，可以对该关联pageId做多大的贡献值 ！！！ 很重要的一点
      }
      // contributions 结果应该是(相邻pageId, 贡献值)
      // 针对每个page，再次计算他们的rank值 0.15 + 0.85 * contributionsReceived
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
      // 计算新的rank: 对于将所有的关联id规约求出id对应的分数值，然后再带入公式计算。
    } // 继续迭代，links是不变的，ranks一直在变


    ranks.saveAsTextFile("ranks")

  }

}
