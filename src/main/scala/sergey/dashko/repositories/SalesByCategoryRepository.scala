package sergey.dashko.repositories

import sergey.dashko.models.SaleByCategory

trait SalesByCategoryRepository extends Repository [SaleByCategory] {
  override type ID = Long
}
