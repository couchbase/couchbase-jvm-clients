package com.couchbase.client.scala

import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.json.JsonObject
import org.scalatest.FunSuite

class JsonObjectSpec extends FunSuite {

  val raw =
    """{"name":"John Smith",
      |"age":29,
      |"address":[{"address":"123 Fake Street","regional":{"county:":"essex"}}]}""".stripMargin
  val json = JsonObject.fromJson(raw)


  test("decode") {
    val encoded = Conversions.encode(json).get
    val str = new String(encoded._1)
    val decoded = Conversions.decode[JsonObject](encoded._1, Conversions.JsonFlags).get

    assert (decoded.str("name") == "John Smith")
  }

  test("int to everything") {
    val r = """{"hello":29}"""
    val j = JsonObject.fromJson(r)
    assert(j.num("hello") == 29)
    assert(j.numLong("hello") == 29)
    assert(j.numDouble("hello") == 29)
    assert(j.numFloat("hello") == 29)
    assert(j.str("hello") == "29")
    assertThrows[RuntimeException](j.bool("hello"))
    assertThrows[RuntimeException](j.obj("hello"))
    assertThrows[RuntimeException](j.arr("hello"))
    assert(j.toString == r)
  }

  test("double to everything") {
    val r = """{"hello":29.34}"""
    val j = JsonObject.fromJson(r)
    assert(j.num("hello") == 29)
    assert(j.numLong("hello") == 29)
    assert(j.numDouble("hello") == 29.34)
    assert(j.numFloat("hello") == 29.34f)
    assert(j.str("hello") == "29.34")
    assertThrows[RuntimeException](j.bool("hello"))
    assertThrows[RuntimeException](j.obj("hello"))
    assertThrows[RuntimeException](j.arr("hello"))
    assert(j.toString == r)
  }

  test("obj to everything") {
    val r = """{"hello":{"foo":"bar"}}"""
    val j = JsonObject.fromJson(r)
    assert(j.obj("hello").nonEmpty)
    assertThrows[RuntimeException](j.num("hello"))
    assertThrows[RuntimeException](j.numLong("hello"))
    assertThrows[RuntimeException](j.numDouble("hello"))
    assertThrows[RuntimeException](j.numFloat("hello"))
    assertThrows[RuntimeException](j.bool("hello"))
    assertThrows[RuntimeException](j.arr("hello"))
    assert(j.toString == r)
  }

  test("arr to everything") {
    val r = """{"hello":["foo","bar"]}"""
    val j = JsonObject.fromJson(r)
    assert(j.arr("hello").nonEmpty)
    assertThrows[RuntimeException](j.num("hello"))
    assertThrows[RuntimeException](j.numLong("hello"))
    assertThrows[RuntimeException](j.numDouble("hello"))
    assertThrows[RuntimeException](j.numFloat("hello"))
    assertThrows[RuntimeException](j.bool("hello"))
    assertThrows[RuntimeException](j.obj("hello"))
    assert(j.toString == r)
  }
}
