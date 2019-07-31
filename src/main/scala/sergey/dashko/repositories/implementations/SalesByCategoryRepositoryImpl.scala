package sergey.dashko.repositories.implementations

import sergey.dashko.models.SaleByCategory
import sergey.dashko.repositories.SalesByCategoryRepository
import doobie.implicits._

class SalesByCategoryRepositoryImpl extends SalesByCategoryRepository {
  override def table: String = "sales_by_category"

  override val columns: List[String] = List("category", "sales_count")

  override def findOneWhereObject(obj: SaleByCategory): doobie.Fragment = fr"Where id = ${obj.id}"

  override def findOneWhere(id: Long): doobie.Fragment = fr"Where id = $id"

  override protected def columnValues(obj: SaleByCategory): doobie.Fragment = fr"(${obj.category}, ${obj.salesCount})"
}
