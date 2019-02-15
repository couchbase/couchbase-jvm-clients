package com.couchbase.client.scala

import com.couchbase.client.scala.json.JsonObject
import org.scalatest.{FlatSpec, FunSuite}

class JsonObjectSpec extends FunSuite {

  val raw =
    """{"name":"John Smith",
      |"age":29,
      |"address":[{"address":"123 Fake Street","regional":{"county:":"essex"}}]}""".stripMargin
  val json = JsonObject.fromJson(raw).get


  test("root incorrectly read as object") {
    assert(json.dyn.address.obj.isFailure)
  }

  test("name.str") {
    assert(json.dyn.name.str.get == "John Smith")
  }

  test("name.int") {
    assert(json.dyn.name.int.isFailure)
  }

  test("age.int") {
    assert(json.dyn.age.int.get == 29)
  }

  test("age.double") {
    assert(json.dyn.age.double.get == 29.0)
  }

  test("age.str") {
    assert(json.dyn.age.str.get == "29")
  }


  test("address.arr") {
    assert(json.dyn.address.arr.isSuccess)
  }

  test("name.arr") {
    assert(json.dyn.name.arr.isFailure)
  }

  test("name(0).str") {
    assert(json.dyn.name(0).str.isFailure)
  }

  test("address(0).address.str") {
    assert(json.dyn.address(0).address.str.get == "123 Fake Street")
  }

  test("address(0).no_exist.str") {
    assert(json.dyn.address(0).no_exist.str.isFailure)
  }

  test("address(0).obj") {
    assert(json.dyn.address(0).obj.isSuccess)
  }

  test("address(1).obj") {
    assert(json.dyn.address(1).obj.isFailure)
  }

  test("address(-1).obj") {
    assert(json.dyn.address(-1).obj.isFailure)
  }

  test("address(0).regional.obj") {
    assert(json.dyn.address(0).regional.obj.isSuccess)
  }

  test("address(0).regional.county.str") {
    assert(json.dyn.address(0).regional.county.str.get == "essex")
  }

  test("address(0).regional.county.int") {
    assert(json.dyn.address(0).regional.county.int.isFailure)
  }
}
