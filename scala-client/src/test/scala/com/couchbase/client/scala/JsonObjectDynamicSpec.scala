package com.couchbase.client.scala

import com.couchbase.client.core.error.DecodingFailedException
import com.couchbase.client.scala.json.JsonObject
import org.scalatest.{FlatSpec, FunSuite}

class JsonObjectDynamicSpec extends FunSuite {

  val raw =
    """{"name":"John Smith",
      |"age":29,
      |"address":[{"address":"123 Fake Street","regional":{"county:":"essex"}}]}""".stripMargin
  val json = JsonObject.fromJson(raw)
  val jsonSafe = json.safe


  test("safe root incorrectly read as object") {
    assert(jsonSafe.dyn.address.obj.isFailure)
  }

  test("safe name.str") {
    assert(jsonSafe.dyn.name.str.get == "John Smith")
  }

  test("safe name.int") {
    assert(jsonSafe.dyn.name.num.isFailure)
  }

  test("safe age.int") {
    assert(jsonSafe.dyn.age.num.get == 29)
  }

  test("safe age.double") {
    assert(jsonSafe.dyn.age.numDouble.get == 29.0)
  }

  test("safe age.str") {
    assert(jsonSafe.dyn.age.str.get == "29")
  }


  test("safe address.arr") {
    assert(jsonSafe.dyn.address.arr.isSuccess)
  }

  test("safe name.arr") {
    assert(jsonSafe.dyn.name.arr.isFailure)
  }

  test("safe name(0).str") {
    assert(jsonSafe.dyn.name(0).str.isFailure)
  }

  test("safe address(0).address.str") {
    assert(jsonSafe.dyn.address(0).address.str.get == "123 Fake Street")
  }

  test("safe address(0).no_exist.str") {
    assert(jsonSafe.dyn.address(0).no_exist.str.isFailure)
  }

  test("safe address(0).obj") {
    assert(jsonSafe.dyn.address(0).obj.isSuccess)
  }

  test("safe address(1).obj") {
    assert(jsonSafe.dyn.address(1).obj.isFailure)
  }

  test("safe address(-1).obj") {
    assert(jsonSafe.dyn.address(-1).obj.isFailure)
  }

  test("safe address(0).regional.obj") {
    assert(jsonSafe.dyn.address(0).regional.obj.isSuccess)
  }

  test("safe address(0).regional.county.str") {
    assert(jsonSafe.dyn.address(0).regional.county.str.get == "essex")
  }

  test("safe address(0).regional.county.int") {
    assert(jsonSafe.dyn.address(0).regional.county.num.isFailure)
  }



  test("root incorrectly read as object") {
    assertThrows[DecodingFailedException](
      json.dyn.address.obj
    )
  }

  test("name.str") {
    assert(json.dyn.name.str== "John Smith")
  }

  test("name.int") {
    assertThrows[DecodingFailedException](
    json.dyn.name.num
    )
  }

  test("age.int") {
    assert(json.dyn.age.num== 29)
  }

  test("age.double") {
    assert(json.dyn.age.numDouble== 29.0)
  }

  test("age.str") {
    assert(json.dyn.age.str== "29")
  }


  test("address.arr") {
    assertThrows[DecodingFailedException](
      json.dyn.address.arr
    )
  }

  test("name.arr") {
    assertThrows[DecodingFailedException](
      json.dyn.name.arr
      )
  }

  test("name(0).str") {
    assertThrows[DecodingFailedException](
      json.dyn.name(0).str
    )
  }

  test("address(0).address.str") {
    assert(json.dyn.address(0).address.str== "123 Fake Street")
  }

  test("address(0).no_exist.str") {
    assertThrows[DecodingFailedException](
      json.dyn.address(0).no_exist.str
    )
  }

  test("address(0).obj") {
    assertThrows[DecodingFailedException](
      json.dyn.address(0).obj
      )
  }

  test("address(1).obj") {
    assertThrows[DecodingFailedException](
      json.dyn.address(1).obj)
  }

  test("address(-1).obj") {
    assertThrows[DecodingFailedException](
      json.dyn.address(-1).obj)
  }

  test("address(0).regional.obj") {
    assertThrows[DecodingFailedException](
      json.dyn.address(0).regional.obj)
  }

  test("address(0).regional.county.str") {
    assert(json.dyn.address(0).regional.county.str== "essex")
  }

  test("address(0).regional.county.int") {
    assertThrows[DecodingFailedException](
      json.dyn.address(0).regional.county.num)
  }
}
