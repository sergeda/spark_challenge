package sergey.dashko.repositories

import sergey.dashko.models.SumByCountry


trait SumByCountryRepository extends Repository [SumByCountry] {
  override type ID = Long
}
