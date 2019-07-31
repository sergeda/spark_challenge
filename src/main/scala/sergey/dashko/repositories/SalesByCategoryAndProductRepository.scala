package sergey.dashko.repositories

import sergey.dashko.models.SaleByCategoryAndProduct

trait SalesByCategoryAndProductRepository extends Repository [SaleByCategoryAndProduct] {
  override type ID = Long
}
