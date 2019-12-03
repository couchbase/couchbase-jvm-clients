package com.couchbase.client.scala

import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.scala.json.JsonObject
import org.junit.jupiter.api.{Assertions, Test}

import scala.util.{Failure, Success}

class JsonObjectDynamicSpec {

  val raw =
    """{"name":"John Smith",
      |"age":29,
      |"address":[{"address":"123 Fake Street","regional":{"county":"essex"}}]}""".stripMargin
  val json     = JsonObject.fromJson(raw)
  val jsonSafe = json.safe
  @Test
  def safe_root_incorrectly_read_as_object() {
    assert(jsonSafe.dyn.address.obj.isFailure)
  }

  @Test
  def safe_name_str() {
    assert(jsonSafe.dyn.name.str.get == "John Smith")
  }

  @Test
  def safe_name_int() {
    assert(jsonSafe.dyn.name.num.isFailure)
  }

  @Test
  def safe_age_int() {
    assert(jsonSafe.dyn.age.num.get == 29)
  }

  @Test
  def safe_age_double() {
    assert(jsonSafe.dyn.age.numDouble.get == 29.0)
  }

  @Test
  def safe_age_str() {
    assert(jsonSafe.dyn.age.str.get == "29")
  }
  @Test
  def safe_address_arr() {
    assert(jsonSafe.dyn.address.arr.isSuccess)
  }

  @Test
  def safe_name_arr() {
    assert(jsonSafe.dyn.name.arr.isFailure)
  }

  @Test
  def safe_name_0_str() {
    assert(jsonSafe.dyn.name(0).str.isFailure)
  }

  @Test
  def safe_address_0_address_str() {
    assert(jsonSafe.dyn.address(0).address.str.get == "123 Fake Street")
  }

  @Test
  def safe_address_0_no_exist_str() {
    assert(jsonSafe.dyn.address(0).no_exist.str.isFailure)
  }

  @Test
  def safe_address_0_obj() {
    jsonSafe.dyn.address(0).obj match {
      case Success(v)   =>
      case Failure(err) => Assertions.fail(err)
    }
  }

  @Test
  def safe_address_1_obj() {
    assert(jsonSafe.dyn.address(1).obj.isFailure)
  }

  @Test
  def safe_address_minus1_obj() {
    assert(jsonSafe.dyn.address(-1).obj.isFailure)
  }

  @Test
  def safe_address_0_regional_obj() {
    assert(jsonSafe.dyn.address(0).regional.obj.isSuccess)
  }

  @Test
  def safe_address_0_regional_county_str() {
    assert(jsonSafe.dyn.address(0).regional.county.str.get == "essex")
  }

  @Test
  def safe_address_0_regional_county_int() {
    assert(jsonSafe.dyn.address(0).regional.county.num.isFailure)
  }
  @Test
  def root_incorrectly_read_as_object() {
    Assertions.assertThrows(
      classOf[InvalidArgumentException],
      () =>
        (
          json.dyn.address.obj
        )
    )
  }

  @Test
  def name_str() {
    assert(json.dyn.name.str == "John Smith")
  }

  @Test
  def name_int() {
    Assertions.assertThrows(
      classOf[NumberFormatException],
      () =>
        (
          json.dyn.name.num
        )
    )
  }

  @Test
  def age_int() {
    assert(json.dyn.age.num == 29)
  }

  @Test
  def age_double() {
    assert(json.dyn.age.numDouble == 29.0)
  }

  @Test
  def age_str() {
    assert(json.dyn.age.str == "29")
  }
  @Test
  def address_arr() {
    json.dyn.address.arr
  }

  @Test
  def name_arr() {
    Assertions.assertThrows(
      classOf[InvalidArgumentException],
      () =>
        (
          json.dyn.name.arr
        )
    )
  }

  @Test
  def name_0_str() {
    Assertions.assertThrows(
      classOf[InvalidArgumentException],
      () =>
        (
          json.dyn.name(0).str
        )
    )
  }

  @Test
  def address_0_address_str() {
    assert(json.dyn.address(0).address.str == "123 Fake Street")
  }

  @Test
  def address_0_no_exist_str() {
    Assertions.assertThrows(
      classOf[InvalidArgumentException],
      () =>
        (
          json.dyn.address(0).no_exist.str
        )
    )
  }

  @Test
  def address_0_obj() {
    json.dyn.address(0).obj
  }

  @Test
  def address_1_obj() {
    Assertions.assertThrows(classOf[InvalidArgumentException], () => (json.dyn.address(1).obj))
  }

  @Test
  def address_minus1_obj() {
    Assertions.assertThrows(classOf[InvalidArgumentException], () => (json.dyn.address(-1).obj))
  }

  @Test
  def address_0_regional_obj() {
    json.dyn.address(0).regional.obj
  }

  @Test
  def address_0_regional_county_str() {
    assert(json.dyn.address(0).regional.county.str == "essex")
  }

  @Test
  def address_0_regional_county_int() {
    Assertions.assertThrows(
      classOf[NumberFormatException],
      () => (json.dyn.address(0).regional.county.num)
    )
  }
}
