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
  def safe_root_incorrectly_read_as_object(): Unit = {
    assert(jsonSafe.dyn.address.obj.isFailure)
  }

  @Test
  def safe_name_str(): Unit = {
    assert(jsonSafe.dyn.name.str.get == "John Smith")
  }

  @Test
  def safe_name_int(): Unit = {
    assert(jsonSafe.dyn.name.num.isFailure)
  }

  @Test
  def safe_age_int(): Unit = {
    assert(jsonSafe.dyn.age.num.get == 29)
  }

  @Test
  def safe_age_double(): Unit = {
    assert(jsonSafe.dyn.age.numDouble.get == 29.0)
  }

  @Test
  def safe_age_str(): Unit = {
    assert(jsonSafe.dyn.age.str.get == "29")
  }
  @Test
  def safe_address_arr(): Unit = {
    assert(jsonSafe.dyn.address.arr.isSuccess)
  }

  @Test
  def safe_name_arr(): Unit = {
    assert(jsonSafe.dyn.name.arr.isFailure)
  }

  @Test
  def safe_name_0_str(): Unit = {
    assert(jsonSafe.dyn.name(0).str.isFailure)
  }

  @Test
  def safe_address_0_address_str(): Unit = {
    assert(jsonSafe.dyn.address(0).address.str.get == "123 Fake Street")
  }

  @Test
  def safe_address_0_no_exist_str(): Unit = {
    assert(jsonSafe.dyn.address(0).no_exist.str.isFailure)
  }

  @Test
  def safe_address_0_obj(): Unit = {
    jsonSafe.dyn.address(0).obj match {
      case Success(v)   =>
      case Failure(err) => Assertions.fail(err)
    }
  }

  @Test
  def safe_address_1_obj(): Unit = {
    assert(jsonSafe.dyn.address(1).obj.isFailure)
  }

  @Test
  def safe_address_minus1_obj(): Unit = {
    assert(jsonSafe.dyn.address(-1).obj.isFailure)
  }

  @Test
  def safe_address_0_regional_obj(): Unit = {
    assert(jsonSafe.dyn.address(0).regional.obj.isSuccess)
  }

  @Test
  def safe_address_0_regional_county_str(): Unit = {
    assert(jsonSafe.dyn.address(0).regional.county.str.get == "essex")
  }

  @Test
  def safe_address_0_regional_county_int(): Unit = {
    assert(jsonSafe.dyn.address(0).regional.county.num.isFailure)
  }
  @Test
  def root_incorrectly_read_as_object(): Unit = {
    Assertions.assertThrows(
      classOf[InvalidArgumentException],
      () =>
        (
          json.dyn.address.obj
        )
    )
  }

  @Test
  def name_str(): Unit = {
    assert(json.dyn.name.str == "John Smith")
  }

  @Test
  def name_int(): Unit = {
    Assertions.assertThrows(
      classOf[NumberFormatException],
      () =>
        (
          json.dyn.name.num
        )
    )
  }

  @Test
  def age_int(): Unit = {
    assert(json.dyn.age.num == 29)
  }

  @Test
  def age_double(): Unit = {
    assert(json.dyn.age.numDouble == 29.0)
  }

  @Test
  def age_str(): Unit = {
    assert(json.dyn.age.str == "29")
  }
  @Test
  def address_arr(): Unit = {
    json.dyn.address.arr
  }

  @Test
  def name_arr(): Unit = {
    Assertions.assertThrows(
      classOf[InvalidArgumentException],
      () =>
        (
          json.dyn.name.arr
        )
    )
  }

  @Test
  def name_0_str(): Unit = {
    Assertions.assertThrows(
      classOf[InvalidArgumentException],
      () =>
        (
          json.dyn.name(0).str
        )
    )
  }

  @Test
  def address_0_address_str(): Unit = {
    assert(json.dyn.address(0).address.str == "123 Fake Street")
  }

  @Test
  def address_0_no_exist_str(): Unit = {
    Assertions.assertThrows(
      classOf[InvalidArgumentException],
      () =>
        (
          json.dyn.address(0).no_exist.str
        )
    )
  }

  @Test
  def address_0_obj(): Unit = {
    json.dyn.address(0).obj
  }

  @Test
  def address_1_obj(): Unit = {
    Assertions.assertThrows(classOf[InvalidArgumentException], () => (json.dyn.address(1).obj))
  }

  @Test
  def address_minus1_obj(): Unit = {
    Assertions.assertThrows(classOf[InvalidArgumentException], () => (json.dyn.address(-1).obj))
  }

  @Test
  def address_0_regional_obj(): Unit = {
    json.dyn.address(0).regional.obj
  }

  @Test
  def address_0_regional_county_str(): Unit = {
    assert(json.dyn.address(0).regional.county.str == "essex")
  }

  @Test
  def address_0_regional_county_int(): Unit = {
    Assertions.assertThrows(
      classOf[NumberFormatException],
      () => (json.dyn.address(0).regional.county.num)
    )
  }
}
