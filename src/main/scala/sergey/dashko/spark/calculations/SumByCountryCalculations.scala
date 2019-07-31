package sergey.dashko.spark.calculations

import cats.effect.IO
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, SparkSession}
import sergey.dashko.{Event, SubnetInfo}

import scala.util.Try


class SumByCountryCalculations(dataset: Dataset[Event], spark: SparkSession, subnetData: RDD[String]) extends Serializable {

  type Subnet = String
  type CountryId = String

  import spark.implicits._

  val subnetDataParsed = subnetData.map(record => parseLine(record.split(",")))


  //Geo task solving using RDD and broadcast subnet Map - quickest
  def sumByCountryRddBroadcast(count: Int): IO[List[(CountryId, Double)]] =
    IO {
      val bcSubnetTable: Broadcast[collection.Map[CountryId, Iterable[Subnet]]] =
        spark.sparkContext.broadcast(subnetDataParsed.groupByKey().collectAsMap)
      val findCountryId: Event => Option[CountryId] = (event) =>
        bcSubnetTable.value.aggregate[Option[CountryId]](None)((acc, elem) => if (acc.nonEmpty) acc else if (ipInSubnets(event.ip, elem._2)) Some(elem._1) else None, (a, b) => a.orElse(b))
      val sums: RDD[(CountryId, Double)] = dataset.rdd.map(event => (findCountryId(event), event.sum)).collect { case (Some(countryId), sum) => (countryId, sum) }
      sums.groupByKey().mapValues(_.sum).sortBy(-_._2).take(count).toList
    }

  //Geo task solving using RDD and cartesian join - second quickest
  def sumByCountryRddCartesian(count: Int): IO[List[(CountryId, Double)]] = IO {
    subnetDataParsed.groupByKey()
      .cartesian(dataset.rdd)
      .filter { case ((_, subnets), event) => subnets.exists(subnet => ipInSubnet(event.ip, subnet)) }
      .map { case ((countryId, _), event) => (countryId, event.sum) }
      .groupByKey()
      .mapValues(_.sum)
      .sortBy(-_._2)
      .take(count)
      .toList
  }

  //Geo task solving using datasets crossJoin - slowest
  def sumByCountryDatasetsCrossJoin(count: Int): IO[List[(CountryId, Double)]] = IO {
        val subnetDataSet: Dataset[SubnetInfo] = subnetDataParsed.map(data => SubnetInfo(data._2, data._1)).toDF().as[SubnetInfo]
        val crossJoin: DataFrame = subnetDataSet.crossJoin(dataset)
        val groupedDataset: RelationalGroupedDataset = crossJoin.filter(row => ipInSubnet(row.getAs[String]("ip"), row.getAs[String]("network")))
          .map( row => (row.getAs[CountryId]("countryId"), row.getAs[Double]("sum"))).toDF("countryId", "sum")
          .groupBy("countryId")
        groupedDataset.sum("sum").toDF("countryId", "sum").sort($"sum".desc).as[(CountryId, Double)].take(count).toList
  }

  private def ipInSubnet(ip: String, lan: Subnet): Boolean = {
    ip.substring(0, 1) == lan.substring(0, 1) && {
      val subnet = Try(new SubnetUtils(lan).getInfo)
      subnet.map(result => result.isInRange(ip)).getOrElse(false)
    }
  }

  private def ipInSubnets(ip: String, subnets: Iterable[Subnet]): Boolean = subnets.exists(subnet => ipInSubnet(ip, subnet))

  private def parseLine(i: Array[String]): (CountryId, Subnet) = (i(2), i(0))
}
