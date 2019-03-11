package com.couchbase.client.scala

import com.couchbase.client.scala.query.QueryOptions
import org.scalatest.FunSuite

class QueryOptionsSpec extends FunSuite {
  test("named params single") {
    val qo = QueryOptions().namedParameters("key1" -> "value1")
    assert(qo.namedParameters.get("key1") == "value1")
  }

  test("named params 3") {
    val qo = QueryOptions().namedParameters("key1" -> "value1", "key2" -> "value2", "key3" -> "value3")
    assert(qo.namedParameters.get("key1") == "value1")
    assert(qo.namedParameters.get("key2") == "value2")
    assert(qo.namedParameters.get("key3") == "value3")
  }
}
