package sergey.dashko.utils

//import cats.effect.
import cats.effect.{Async, ContextShift, Resource}
//import cats.implicits._
import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.HikariConfig
import doobie.util.ExecutionContexts
import doobie._
import doobie.implicits._
import doobie.hikari.HikariTransactor

import scala.util.Try

trait Db[F[_]] {
  def transactor: Resource[F, HikariTransactor[F]]
  def transact[T](connectionIO: ConnectionIO[T]): F[T]
}

class DbImpl[F[_]: Async: ContextShift] extends Db[F] {
  private val c = ConfigFactory.load()
  val config = new HikariConfig()
  config.setJdbcUrl(c.getString("db.default.url"))
  config.setUsername(c.getString("db.default.username"))
  config.setDriverClassName(c.getString("db.default.driver"))
  config.setMaximumPoolSize(c.getInt("doobie.poolMaxSize"))
  Try(c.getString("db.default.password")).foreach { pass =>
    config.setPassword(pass)
  }

  val transactor: Resource[F, HikariTransactor[F]] =
    for {
      ce <- ExecutionContexts.fixedThreadPool[F](c.getInt("doobie.poolMaxSize") * 10) // our connect EC
      te <- ExecutionContexts.cachedThreadPool[F] // our transaction EC
      xa <- HikariTransactor.fromHikariConfig[F](
        config, // password
        ce, // await connection here
        te // execute JDBC operations here
      )
    } yield xa

  def transact[T](connectionIO: ConnectionIO[T]): F[T] = transactor.use { tx =>
    connectionIO.transact(tx)
  }

}
