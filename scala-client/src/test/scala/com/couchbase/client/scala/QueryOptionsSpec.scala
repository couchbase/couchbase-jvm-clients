package com.couchbase.client.scala

import com.couchbase.client.scala.query.QueryOptions
import org.junit.jupiter.api.Test

class QueryOptionsSpec {
  @Test
  def named_params_single() {
    val qo = QueryOptions().parameters(Map("key1" -> "value1"))
    assert(qo.namedParameters.get("key1") == "value1")
    assert(qo.deferredException.isEmpty)
  }

  @Test
  def named_params_3() {
    val qo =
      QueryOptions().parameters(Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3"))
    assert(qo.namedParameters.get("key1") == "value1")
    assert(qo.namedParameters.get("key2") == "value2")
    assert(qo.namedParameters.get("key3") == "value3")
    assert(qo.deferredException.isEmpty)
  }

  @Test
  def badPositionalArgs(): Unit = {
    case class NotJson()

    val qo =
      QueryOptions().parameters(Map("key1" -> "value1", "key2" -> NotJson(), "key3" -> "value3"))
    assert(qo.deferredException.isDefined)
  }

  @Test
  def badNamedArgs(): Unit = {
    case class NotJson()

    val qo = QueryOptions().parameters(Seq("key1", NotJson(), 5))
    assert(qo.deferredException.isDefined)
  }
}
