import com.ua.Entity.{ProductStats, ProductData}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class SparkTest extends FunSuite {
  val sparkSession = SparkSession.builder
    .appName("myApp")
    .getOrCreate()

  import sparkSession.implicits._

  val statistics = new ProductStats

  val list = List(
    ProductData("Afghanistan", 51, "Production Quantity", 1994, "tonnes", 17500, "F", "apples"),
    ProductData("Afghanistan", 31, "Area Harvested", 2007, "Ha", 2350, "F", "apples"),
    ProductData("Zimbabwe", 51, "Production Quantity", 1980, "tonnes", 0, "NR", "anise_badian_fennel_corian"),
    ProductData("Yugoslav SFR", 51, "Production Quantity", 1965, "tonnes", 41500, "blanck", "cherries"),
    ProductData("Algeria", 51, "Production Quantity", 2001, "tonnes", 104900, "blanck", "apples"),
    ProductData("Algeria", 51, "Production Quantity", 1972, "tonnes", 11480, "blanck", "apples"),
    ProductData("Armenia", 51, "Production Quantity", 2001, "tonnes", 35380, "blanck", "apples"),
    ProductData("Australia", 51, "Production Quantity", 1964, "tonnes", 367397, "blanck", "apples"),
    ProductData("Albania", 51, "Production Quantity", 1965, "tonnes", 0, "NR", "other_melons_inc_cantaloupes")
  )

  test("total production") {
    val input: Dataset[ProductData] = sparkSession.createDataset[ProductData](list)
    val result = statistics.getTotalProduction(input, "apples", 2001).head().getAs[Long](0)
    val expected = 140280
    assert(expected == result)
  }

  test("top productive year") {
    val input: Dataset[ProductData] = sparkSession.createDataset[ProductData](list)
    val result = statistics.getTopYear(input, "apples", "Algeria").head().year
    val expected = 2001
    assert(expected == result)
  }

  test("compare top producers") {
    val input: Dataset[ProductData] = sparkSession.createDataset[ProductData](list)
    val result = statistics.getTopProducers(input, "apples", 2001).select("country_or_area").take(10).map(row => row.getString(0)).toList
    val expected = List("Algeria", "Armenia")
    assert(expected == result)
  }

}
