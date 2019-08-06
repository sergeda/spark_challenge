package sergey.dashko.spark.calculations

import cats.effect.IO
import org.apache.spark.sql.{Dataset, SparkSession}
import sergey.dashko.Event
import sergey.dashko.Main.timed

class SalesByCategoryAndProductCalculations(dataset: Dataset[Event], spark: SparkSession) extends Serializable {

  import spark.implicits._

  val countSales = (events: Seq[Event]) => events.foldLeft(Map.empty[String, Int])((accum, event) =>
    accum.get(event.good).fold(accum + (event.good -> 1))(currentCount => accum + (event.good -> (currentCount + 1))))


  def salesByCategoryAndProductRdd(count: Int): IO[List[(String, List[(String, Int)])]] = {
    IO {
      timed("salesByCategoryAndProductRdd",
      dataset.rdd
        .groupBy(_.category)
        .mapValues(events =>
          countSales(events.toSeq)
            .toList
            .sortBy(-_._2)
            .take(count)
        ).collect().toList
      )
    }
  }

  def salesByCategoryAndProductDataset(count: Int): IO[List[(String, List[(String, Int)])]] = {
    IO {
      timed("salesByCategoryAndProductDataset",
      dataset.groupByKey(_.category)
        .mapGroups { case (category, events) =>
          (category, countSales(events.toSeq).toList.sortBy(-_._2).take(count))
        }
        .collect().toList
      )
    }
  }
}

