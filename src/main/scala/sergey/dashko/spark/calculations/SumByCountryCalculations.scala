package sergey.dashko.spark.calculations

import cats.effect.IO
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.{Partitioner, RangePartitioner}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, SparkSession}
import sergey.dashko.{Event, SubnetInfo}
import sergey.dashko.Main.timed
import scala.reflect.ClassTag
import scala.util.Try

class IpAddressPartitioner[K : Ordering : ClassTag, V](partitionsNumber: Int, rdd: RDD[(K, V)]) extends Partitioner {
  private val rangePartitioner = new RangePartitioner[K, V](partitionsNumber, rdd)
  override def numPartitions: Int = partitionsNumber

  override def getPartition(key: Any): Int = {
    val string = key.asInstanceOf[String]
    if(string.length > 0) rangePartitioner.getPartition(string.substring(0,1)) else rangePartitioner.getPartition(string)
  }
}
class SumByCountryCalculations(dataset: Dataset[Event], spark: SparkSession, subnetData: RDD[String]) extends Serializable {

  import spark.implicits._

  type Subnet = String
  type CountryId = Int

  import spark.implicits._

  val subnetDataParsed: RDD[(CountryId, Subnet)] = {
    subnetData.map(record => parseLine(record.split(","))).collect{case Some(v) => (v._1, v._2)}
  }


  //1# Geo task solving using RDD and broadcast subnet Map - quickest
  def sumByCountryRddBroadcast(count: Int): IO[List[(CountryId, Double)]] =
    IO {
      timed("sumByCountryRddBroadcast", {
        val bcSubnetTable: Broadcast[collection.Map[CountryId, Iterable[Subnet]]] =
          spark.sparkContext.broadcast(subnetDataParsed.groupByKey().collectAsMap)
        val findCountryId: Event => Option[CountryId] = (event) =>
          bcSubnetTable.value.aggregate[Option[CountryId]](None)((acc, elem) => if (acc.nonEmpty) acc else if (ipInSubnets(event.ip, elem._2)) Some(elem._1) else None, (a, b) => a.orElse(b))
        val sums: RDD[(CountryId, Double)] = dataset.rdd.map(event => (findCountryId(event), event.sum)).collect { case (Some(countryId), sum) => (countryId, sum) }
        sums.groupByKey().mapValues(_.sum).sortBy(-_._2).take(count).toList
      }
      )
    }

  //#2 Geo task solving using RDD and cartesian join - second quickest
  def sumByCountryRddCartesian(count: Int): IO[List[(CountryId, Double)]] = IO {
    val addToMap = (map: Map[CountryId, Double], toAdd: Double, countryId: CountryId) => map.get(countryId).fold(map + (countryId -> toAdd))(currentValue => map + (countryId -> (currentValue + toAdd)))
    val seqOp = (accum: Map[CountryId, Double], element: ((Subnet, CountryId), (String, Double))) => if(ipInSubnet(element._2._1, element._1._1)) addToMap(accum, element._2._2, element._1._2) else accum
    val combOp = (m1: Map[CountryId, Double], m2: Map[CountryId, Double]) => {
      val elements1: List[(CountryId, Double)] = m1.toList
      val elements2: List[(CountryId, Double)] = m2.toList
      (elements1 ++ elements2).groupBy(_._1).mapValues(_.map{ case (_, v) => v}.sum)
    }

    timed("sumByCountryRddCartesian",
      {
        val flipped = subnetDataParsed.map(_.swap)
        val partitioner = new IpAddressPartitioner(flipped.getNumPartitions, flipped)
        val eventRdd = dataset.rdd.map{ event => (event.ip, event.sum)}.partitionBy(partitioner).persist()
        val partitionedEventRdd = flipped.partitionBy(partitioner).persist()
        partitionedEventRdd.cartesian(eventRdd).aggregate(Map.empty[CountryId, Double])(seqOp, combOp)
        .toList
        .sortBy(-_._2)
        .take(count)
      }
    )
  }

  //#3 Geo task solving using datasets crossJoin - slowest
  def sumByCountryDatasetsCrossJoin(count: Int): IO[List[(CountryId, Double)]] = IO {
    timed("sumByCountryDatasetsCrossJoin",
      {
        val subnetDataSet: Dataset[SubnetInfo] = subnetDataParsed.map(data => SubnetInfo(data._2, data._1)).toDS()
        val crossJoin: DataFrame = subnetDataSet.crossJoin(dataset)
        val groupedDataset: RelationalGroupedDataset = crossJoin.filter(row => ipInSubnet(row.getAs[String]("ip"), row.getAs[String]("network")))
          .map(row => (row.getAs[CountryId]("countryId"), row.getAs[Double]("sum"))).toDF("countryId", "sum")
          .groupBy("countryId")
        val ds = groupedDataset.sum("sum").toDF("countryId", "sum").sort($"sum".desc).as[(CountryId, Double)]
        ds.take(count).toList
      })
  }

  //#4 Geo task solving using sql and UDF - same as #2
  def sumByCountrySql(count: Int): IO[List[(CountryId, Double)]] = IO {

    timed("sumByCountrySql",
      {
        spark.udf.register( "ipInSubnet", ipInSubnet _ )
        subnetDataParsed.toDF("countryId", "subnet").createOrReplaceTempView("subnets")
        dataset.map( event => (event.ip, event.sum)).toDF("ip", "sum").createOrReplaceTempView("events")
        val df = spark.sql(s"SELECT subnets.countryId, SUM(events.sum) as sum FROM events join subnets ON true where ipInSubnet(events.ip, subnets.subnet) group by subnets.countryId order by sum desc limit ${count}")
            .as[(CountryId, Double)]
        df.collect().toList
      })
  }

  private def ipInSubnet(ip: String, lan: Subnet): Boolean = {
    ip.substring(0, 1) == lan.substring(0, 1) && {
      val subnet = Try(new SubnetUtils(lan).getInfo)
      subnet.map(result => result.isInRange(ip)).getOrElse(false)
    }
  }

  private def ipInSubnets(ip: String, subnets: Iterable[Subnet]): Boolean = subnets.exists(subnet => ipInSubnet(ip, subnet))

  private def parseLine(i: Array[String]): Option[(CountryId, Subnet)] = Try(i(2).toInt).toOption.map((_, i(0)))
}
