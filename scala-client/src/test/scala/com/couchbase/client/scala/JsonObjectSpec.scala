package com.couchbase.client.scala

import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.json.JsonObject
import org.scalatest.FunSuite

class JsonObjectSpec extends FunSuite {

  val raw =
    """{"name":"John Smith",
      |"age":29,
      |"address":[{"address":"123 Fake Street","regional":{"county:":"essex"}}]}""".stripMargin
  val json = JsonObject.fromJson(raw).get


  test("decode") {
    val encoded = Conversions.encode(json).get
    val str = new String(encoded._1)
    val decoded = Conversions.decode[JsonObject](encoded._1, Conversions.JsonFlags).get

    assert (decoded.str("name") == "John Smith")
  }

}
