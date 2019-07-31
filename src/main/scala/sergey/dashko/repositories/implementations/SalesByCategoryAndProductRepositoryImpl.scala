package sergey.dashko.repositories.implementations

import doobie.implicits._
import sergey.dashko.models.SaleByCategoryAndProduct
import sergey.dashko.repositories.SalesByCategoryAndProductRepository

class SalesByCategoryAndProductRepositoryImpl extends SalesByCategoryAndProductRepository {
  override def table: String = "sales_by_product"

  override val columns: List[String] = List("category", "title", "sales_count")

  override def findOneWhereObject(obj: SaleByCategoryAndProduct): doobie.Fragment = fr"Where id = ${obj.id}"

  override def findOneWhere(id: Long): doobie.Fragment = fr"Where id = $id"

  override protected def columnValues(obj: SaleByCategoryAndProduct): doobie.Fragment = fr"(${obj.category}, ${obj.product}, ${obj.salesCount})"
}
