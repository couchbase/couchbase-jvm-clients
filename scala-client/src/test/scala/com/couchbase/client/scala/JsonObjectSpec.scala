package com.couchbase.client.scala

import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.json.JsonObject
import org.junit.jupiter.api.{Assertions, Test}

class JsonObjectSpec {

  val raw =
    """{"name":"John Smith",
      |"age":29,
      |"address":[{"address":"123 Fake Street","regional":{"county:":"essex"}}]}""".stripMargin
  val json = JsonObject.fromJson(raw)
  @Test
  def decode(): Unit = {
    val encoded = Conversions.encode(json).get
    val str     = new String(encoded)
    val decoded = Conversions.decode[JsonObject](encoded).get

    assert(decoded.str("name") == "John Smith")
  }

  @Test
  def int_to_everything(): Unit = {
    val r = """{"hello":29}"""
    val j = JsonObject.fromJson(r)
    assert(j.num("hello") == 29)
    assert(j.numLong("hello") == 29)
    assert(j.numDouble("hello") == 29)
    assert(j.numFloat("hello") == 29)
    assert(j.str("hello") == "29")
    Assertions.assertThrows(classOf[RuntimeException], () => (j.bool("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.obj("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.arr("hello")))
    assert(j.toString == r)
  }

  @Test
  def double_to_everything(): Unit = {
    val r = """{"hello":29.34}"""
    val j = JsonObject.fromJson(r)
    assert(j.num("hello") == 29)
    assert(j.numLong("hello") == 29)
    assert(j.numDouble("hello") == 29.34)
    assert(j.numFloat("hello") == 29.34f)
    assert(j.str("hello") == "29.34")
    Assertions.assertThrows(classOf[RuntimeException], () => (j.bool("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.obj("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.arr("hello")))
    assert(j.toString == r)
  }

  @Test
  def obj_to_everything(): Unit = {
    val r = """{"hello":{"foo":"bar"}}"""
    val j = JsonObject.fromJson(r)
    assert(j.obj("hello").nonEmpty)
    Assertions.assertThrows(classOf[RuntimeException], () => (j.num("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.numLong("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.numDouble("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.numFloat("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.bool("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.arr("hello")))
    assert(j.toString == r)
  }

  @Test
  def arr_to_everything(): Unit = {
    val r = """{"hello":["foo","bar"]}"""
    val j = JsonObject.fromJson(r)
    assert(j.arr("hello").nonEmpty)
    Assertions.assertThrows(classOf[RuntimeException], () => (j.num("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.numLong("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.numDouble("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.numFloat("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.bool("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.obj("hello")))
    assert(j.toString == r)
  }
}
