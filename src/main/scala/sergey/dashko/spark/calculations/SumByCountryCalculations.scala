package sergey.dashko.spark.calculations

import cats.effect.IO
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._
import org.apache.spark.{Partitioner, RangePartitioner}
import sergey.dashko.Main.timed
import sergey.dashko.{Event, EventDataShort, SubnetInfo}

import scala.collection.mutable.{Map => MMAP}
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

case class CombinedData(ip: String, sum: Double, network: String, countryId: Int)

class SumByCountryCalculations(dataset: Dataset[Event], spark: SparkSession, subnetData: RDD[String]) extends Serializable {

  type Subnet = String
  type CountryId = Int

  import spark.implicits._

  val subnetDataParsed: RDD[(CountryId, Subnet)] = {
    subnetData.map(record => parseLine(record.split(","))).collect{case Some(v) => (v._1, v._2)}
  }

  val combOp: (MMAP[CountryId, Double], MMAP[CountryId, Double]) => MMAP[CountryId, Double] = (m1: MMAP[CountryId, Double], m2: MMAP[CountryId, Double]) => {
    val elements1: List[(CountryId, Double)] = m1.toList
    val elements2: List[(CountryId, Double)] = m2.toList
    MMAP((elements1 ++ elements2).groupBy(_._1).mapValues(_.map{ case (_, v) => v}.sum).toSeq: _*)
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
    val addToMap = (map: MMAP[CountryId, Double], toAdd: Double, countryId: CountryId) => map.get(countryId).fold(map += (countryId -> toAdd))(currentValue => map += (countryId -> (currentValue + toAdd)))
    val seqOp = (accum: MMAP[CountryId, Double], element: ((Subnet, CountryId), (String, Double))) => if(ipInSubnet(element._2._1, element._1._1)) addToMap(accum, element._2._2, element._1._2) else accum

    timed("sumByCountryRddCartesian",
      {
        val flipped = subnetDataParsed.map(_.swap)
        val partitioner = new IpAddressPartitioner(flipped.getNumPartitions, flipped)
        val eventRdd = dataset.rdd.map{ event => (event.ip, event.sum)}.partitionBy(partitioner).persist()
        val partitionedEventRdd = flipped.partitionBy(partitioner).persist()
        partitionedEventRdd.cartesian(eventRdd).aggregate(MMAP.empty[CountryId, Double])(seqOp, combOp)
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


  //#5 Geo task solving using datasets crossJoin aggregator
  def sumByCountryDatasetsCrossJoinAggregator(count: Int): IO[List[(CountryId, Double)]] = IO {
    timed("sumByCountryDatasetsCrossJoinAggregator",
      {
        val eventData: Dataset[EventDataShort] = dataset.map(event => EventDataShort(event.ip, event.sum))
        val subnetDataSet: Dataset[SubnetInfo] = subnetDataParsed.map(data => SubnetInfo(data._2, data._1)).toDS()
        val combinedData: Dataset[CombinedData] = subnetDataSet.crossJoin(eventData).as[CombinedData]
        val aggregator = new Aggregator[CombinedData, MMAP[CountryId, Double], List[(CountryId, Double)]] {

          val mapEncoder: ExpressionEncoder[MMAP[CountryId, Double]] = ExpressionEncoder()
          val listEncoder: ExpressionEncoder[List[(CountryId, Double)]] = ExpressionEncoder()

          override def zero: MMAP[CountryId, Double] = MMAP.empty[CountryId, Double]

          override def reduce(b: MMAP[CountryId, Double], a: CombinedData): MMAP[CountryId, Double] = {
            if(ipInSubnet(a.ip, a.network)){
              b.get(a.countryId).map( current => b += (a.countryId -> (current + a.sum)))
                .getOrElse(b += (a.countryId -> a.sum))
            } else b

          }

          override def merge(b1: MMAP[CountryId, Double], b2: MMAP[CountryId, Double]): MMAP[CountryId, Double] = combOp(b1, b2)

          override def bufferEncoder: Encoder[MMAP[CountryId, Double]] = mapEncoder

          override def finish(reduction: MMAP[CountryId, Double]): List[(CountryId, Double)] = reduction.toList.sortBy(- _._2)

          override def outputEncoder: Encoder[List[(CountryId, Double)]] = listEncoder
        }
          combinedData.select(aggregator.toColumn).collect.toList.flatten.take(count)
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
