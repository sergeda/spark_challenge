package sergey.dashko.spark.calculations

import cats.effect.IO
import org.apache.spark.sql.{Dataset, SparkSession}
import sergey.dashko.Event

case class SaleCountByCategory(category: String, salesCount: Int)

class SalesByCategoryCalculations(dataset: Dataset[Event], spark: SparkSession) {

  import spark.implicits._

  def salesByCategoryRdd(count: Int): IO[List[(String, Int)]] = {
    IO(
      dataset.rdd
        .groupBy(_.category)
        .mapValues(_.size)
        .sortBy(_._2)
        .take(count)
        .toList
    )
  }

  def salesByCategoryDataset(count: Int): IO[List[SaleCountByCategory]] = {
    IO(
    dataset.groupByKey(_.category)
      .mapGroups { case (category, events) => (category, events.length) }.as[SaleCountByCategory]
      .sort($"salesCount".desc)
      .limit(count)
      .collect()
      .toList
    )
  }
}
