package com.couchbase.client.scala

import com.couchbase.client.scala.query.QueryOptions
import org.junit.jupiter.api.Test

class QueryOptionsSpec {
  @Test
  def named_params_single() {
    val qo = QueryOptions().namedParameters(Map("key1" -> "value1"))
    assert(qo.namedParameters.get("key1") == "value1")
  }

  @Test
  def named_params_3() {
    val qo = QueryOptions().namedParameters(Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3"))
    assert(qo.namedParameters.get("key1") == "value1")
    assert(qo.namedParameters.get("key2") == "value2")
    assert(qo.namedParameters.get("key3") == "value3")
  }
}
