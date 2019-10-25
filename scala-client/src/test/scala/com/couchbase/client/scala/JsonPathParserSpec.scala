package com.couchbase.client.scala

import com.couchbase.client.scala.json.{PathArray, PathObjectOrField}
import com.couchbase.client.scala.kv.JsonPathParser
import org.junit.jupiter.api.Test

import scala.util.Success

class JsonPathParserSpec {
  @Test
  def name() {
    assert(JsonPathParser.parse("name") == Success(Seq(PathObjectOrField("name"))))
  }

  @Test
  def foo_2() {
    assert(JsonPathParser.parse("foo[2]").get == Seq(PathArray("foo", 2)))
  }

  @Test
  def foo_bar() {
    assert(
      JsonPathParser.parse("foo.bar").get == Seq(PathObjectOrField("foo"), PathObjectOrField("bar"))
    )
  }

  @Test
  def foo_bar_2() {
    assert(
      JsonPathParser.parse("foo.bar[2]").get == Seq(PathObjectOrField("foo"), PathArray("bar", 2))
    )
  }

  @Test
  def foo_2_bar() {
    assert(
      JsonPathParser.parse("foo[2].bar").get == Seq(PathArray("foo", 2), PathObjectOrField("bar"))
    )
  }

  @Test
  def foo_9999_bar() {
    assert(
      JsonPathParser
        .parse("foo[9999].bar")
        .get == Seq(PathArray("foo", 9999), PathObjectOrField("bar"))
    )
  }

  @Test
  def foo_2_bar_80_baz() {
    assert(
      JsonPathParser
        .parse("foo[2].bar[80].baz")
        .get == Seq(PathArray("foo", 2), PathArray("bar", 80), PathObjectOrField("baz"))
    )
  }

  @Test
  def bad_idx() {
    assert(JsonPathParser.parse("foo[bad]").isFailure)
  }

  @Test
  def missing_idx_end() {
    assert(JsonPathParser.parse("foo[12").isFailure)
  }

  @Test
  def empty() {
    assert(JsonPathParser.parse("") == Success(Seq()))
  }

}
