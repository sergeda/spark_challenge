package sergey.dashko

import cats.effect.{IOApp, _}
import cats.implicits._
import com.typesafe.config.{Config, ConfigFactory}
import doobie.implicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import sergey.dashko.models.{SaleByCategory, SaleByCategoryAndProduct, SumByCountry}
import sergey.dashko.repositories.implementations.{SalesByCategoryAndProductRepositoryImpl, SalesByCategoryRepositoryImpl, SumByCountryRepositoryImpl}
import sergey.dashko.spark.calculations.{SalesByCategoryAndProductCalculations, SalesByCategoryCalculations, SumByCountryCalculations}
import sergey.dashko.utils.DbImpl



object Main extends IOApp {


  val sparkSession: Resource[IO, SparkSession] =
    Resource.make {
      IO(SparkSession
        .builder()
        .appName("Spark project")
        .master("local[*]")
        .getOrCreate())
    } { sparkSession =>
      IO(sparkSession.close()).handleErrorWith(_ => IO.unit)
    }


  val schema = Encoders.product[Event].schema

  def run(args: List[String]): IO[ExitCode] = {
    val config: Config = ConfigFactory.load()
    val db = new DbImpl[IO]
    val salesByCategoryRepo = new SalesByCategoryRepositoryImpl
    val salesByCategoryAndProductRepo = new SalesByCategoryAndProductRepositoryImpl
    val sumByCountryRepo = new SumByCountryRepositoryImpl


    sparkSession.use { spark =>
      import spark.implicits._

      val dataset: IO[Dataset[Event]] = IO(spark
        .read
        .format("csv")
        .option("delimiter", ",")
        .schema(schema)
        .load(config.getString("path-to-events"))
        .as[Event])

      val subnetData = spark.sparkContext.textFile(config.getString("path-to-geoip-data"))
      val header = subnetData.first()
      val subnetDataFiltered: RDD[String] = subnetData.filter(_ != header)

      val salesByCategoryResult: IO[List[(String, Int)]] = for {
        datast <- dataset
        salesByCategoryCalculations = new SalesByCategoryCalculations(datast, spark)
        topTen <- salesByCategoryCalculations.salesByCategoryRdd(10)
        transaction = topTen.map{ tuple => salesByCategoryRepo.insert(SaleByCategory(0, tuple._1, tuple._2))}.sequence
        _ <- db.transact(transaction)
        _ <- IO(println("Top ten categories by sales:\n" + topTen.mkString("\n")))
      } yield topTen

      val salesByCategoryAndProductResult = for {
        datast <- dataset
        calculations = new SalesByCategoryAndProductCalculations(datast, spark)
        categories <- calculations.salesByCategoryAndProductDataset(10)
        transaction = categories.flatMap{ data => data._2.map( sale => salesByCategoryAndProductRepo.insert(SaleByCategoryAndProduct(0, data._1, sale._1, sale._2)))}.sequence
        _ <- db.transact(transaction)
        _ <- IO(println("Top ten products by sales in each category:\n"))
        _ <- IO(categories.foreach { case (category, goods) =>
                  println(s"Category: $category")
                  goods.foreach( good => println(s"${good._1}: ${good._2}"))
                })
      } yield categories

      val sumByCountryResult = for {
        datast <- dataset
        calculations = new SumByCountryCalculations(datast, spark, subnetDataFiltered)
        sumsByCountry <- calculations.sumByCountryRddBroadcast(10)
        transaction = sumsByCountry.map{ tuple => sumByCountryRepo.insert(SumByCountry(0, tuple._1, tuple._2))}.sequence
        _ <- db.transact(transaction)
        _ <- IO(println("Top ten countries by sum of sales:\n" + sumsByCountry.mkString("\n")))
      } yield sumsByCountry



      for {
        _ <- salesByCategoryResult
        _ <- salesByCategoryAndProductResult
        _ <- sumByCountryResult
      } yield ExitCode.Success
    }
  }

}
