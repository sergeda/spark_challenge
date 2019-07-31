package sergey.dashko.repositories

import doobie._
import doobie.implicits._
import doobie.util.fragments.whereAndOpt

trait Repository[T] {

  def table: String
  val columns: List[String]

  type ID

  def findOneWhereObject(obj: T): Fragment
  def findOneWhere(id: ID): Fragment

  def findOne(id: ID)(implicit read: Read[T]): ConnectionIO[Option[T]] = {
    val sql = fr"select * from " ++ tableFragment ++ fr" " ++ findOneWhere(id)
    sql.query[T].option
  }

  def count(where: Option[Fragment]): ConnectionIO[Int] = {
    val baseSql = fr"select COUNT (*) from " ++ tableFragment
    val sql: Fragment = baseSql ++ whereAndOpt(where)
    sql.query[Int].unique
  }

  def delete(id: ID): ConnectionIO[Int] = {
    val sql = fr"DELETE FROM " ++ tableFragment ++ fr" " ++ findOneWhere(id: ID)
    sql.update.run
  }

  def update(obj: T)(implicit read: Read[T]): ConnectionIO[Int] = {
    val sql = fr"UPDATE " ++ tableFragment ++ fr" SET (" ++ columnsFragment ++ fr") =" ++ columnValues(obj) ++ findOneWhereObject(
      obj
    )
    sql.update.run
  }

  def insert(obj: T)(implicit read: Read[T]): ConnectionIO[Int] = {
    val sql = fr"INSERT INTO " ++ tableFragment ++ fr" (" ++ columnsFragment ++ fr") VALUES" ++ columnValues(
      obj
    )
    sql.update.run
  }

  def list(count: Int = 100, offset: Int = 0)(implicit read: Read[T]): ConnectionIO[List[T]] = {
    val sql = fr"SELECT * FROM " ++ tableFragment ++ fr" OFFSET $offset LIMIT $count"
    sql.query[T].to[List]
  }

  protected def columnValues(obj: T): Fragment
  protected lazy val columnsFragment = Fragment.const(columns.mkString(", "))
  lazy val tableFragment = Fragment.const(table)
}
