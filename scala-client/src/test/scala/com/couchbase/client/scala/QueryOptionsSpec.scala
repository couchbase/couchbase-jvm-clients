package com.couchbase.client.scala

import com.couchbase.client.scala.query.{QueryOptions, QueryParameters}
import org.junit.jupiter.api.Test

class QueryOptionsSpec {
  @Test
  def named_params_single(): Unit = {
    val qo = QueryOptions().parameters(QueryParameters.Named("key1" -> "value1"))
    assert(qo.parameters.get.asInstanceOf[QueryParameters.Named].parameters("key1") == "value1")
    assert(qo.deferredException.isEmpty)
  }

  @Test
  def named_params_3(): Unit = {
    val qo =
      QueryOptions().parameters(
        QueryParameters.Named("key1" -> "value1", "key2" -> "value2", "key3" -> "value3")
      )
    val p = qo.parameters.get.asInstanceOf[QueryParameters.Named].parameters
    assert(p("key1") == "value1")
    assert(p("key2") == "value2")
    assert(p("key3") == "value3")
    assert(qo.deferredException.isEmpty)
  }

  @Test
  def badPositionalArgs(): Unit = {
    case class NotJson()

    val qo =
      QueryOptions().parameters(
        QueryParameters.Named("key1" -> "value1", "key2" -> NotJson(), "key3" -> "value3")
      )
    assert(qo.deferredException.isDefined)
  }

  @Test
  def badNamedArgs(): Unit = {
    case class NotJson()

    val qo = QueryOptions().parameters(QueryParameters.Positional("key1", NotJson(), 5))
    assert(qo.deferredException.isDefined)
  }
}
