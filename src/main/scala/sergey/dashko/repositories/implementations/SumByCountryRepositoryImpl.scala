package sergey.dashko.repositories.implementations

import doobie.implicits._
import sergey.dashko.models.{SaleByCategory, SumByCountry}
import sergey.dashko.repositories.SumByCountryRepository

class SumByCountryRepositoryImpl extends SumByCountryRepository {
  override def table: String = "sales_total_by_country"

  override val columns: List[String] = List("country_id", "sum")

  override def findOneWhereObject(obj: SumByCountry): doobie.Fragment = fr"Where id = ${obj.id}"

  override def findOneWhere(id: Long): doobie.Fragment = fr"Where id = $id"

  override protected def columnValues(obj: SumByCountry): doobie.Fragment = fr"(${obj.countryId}, ${obj.total})"
}
