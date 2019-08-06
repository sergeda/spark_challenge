package sergey.dashko.spark.calculations

import cats.effect.IO
import org.apache.spark.sql.{Dataset, SparkSession}
import sergey.dashko.Event
import sergey.dashko.Main.timed

case class SaleCountByCategory(category: String, salesCount: Int)

class SalesByCategoryCalculations(dataset: Dataset[Event], spark: SparkSession) {

  import spark.implicits._

  def salesByCategoryRdd(count: Int): IO[List[(String, Int)]] = {
    val seqOp = (accum: Map[String, Int], event: Event) => accum.get(event.category).fold(accum + (event.category -> 1))(current => accum + (event.category -> (current + 1)))
    val combOp = (accum1: Map[String, Int], accum2: Map[String, Int]) => {
      val elements1: List[(String, Int)] = accum1.toList
      val elements2: List[(String, Int)] = accum2.toList
      (elements1 ++ elements2).groupBy(_._1).mapValues(_.map{ case (_, v) => v}.sum)
    }
    IO {
      timed("salesByCategoryRdd", dataset.rdd.aggregate(Map.empty[String, Int])(seqOp, combOp).toList.sortBy(- _._2))
    }
  }

  def salesByCategoryDataset(count: Int): IO[List[(String, Int)]] = {
    IO {
      timed("salesByCategoryDataset",
        dataset.groupByKey(_.category)
        .mapGroups { case (category, events) => (category, events.length) }.toDF("category", "salesCount").as[SaleCountByCategory]
        .sort($"salesCount".desc)
        .limit(count)
        .collect()
        .toList
        .map(v => (v.category, v.salesCount)))
    }
  }
}
