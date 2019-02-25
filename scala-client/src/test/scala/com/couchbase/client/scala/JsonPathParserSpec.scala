package com.couchbase.client.scala

import com.couchbase.client.scala.json.{PathArray, PathObjectOrField}
import com.couchbase.client.scala.kv.JsonPathParser
import org.scalatest.FunSuite

import scala.util.Success

class JsonPathParserSpec extends FunSuite {
  test("name") {
    assert(JsonPathParser.parse("name") == Success(Seq(PathObjectOrField("name"))))
  }

  test("foo[2]") {
    assert(JsonPathParser.parse("foo[2]").get == Seq(
      PathArray("foo", 2)))
  }

  test("foo.bar") {
    assert(JsonPathParser.parse("foo.bar").get == Seq(
      PathObjectOrField("foo"),
      PathObjectOrField("bar")))
  }

  test("foo.bar[2]") {
    assert(JsonPathParser.parse("foo.bar[2]").get == Seq(
      PathObjectOrField("foo"),
      PathArray("bar", 2)))
  }

  test("foo[2].bar") {
    assert(JsonPathParser.parse("foo[2].bar").get == Seq(
      PathArray("foo", 2),
      PathObjectOrField("bar")))
  }

  test("foo[9999].bar") {
    assert(JsonPathParser.parse("foo[9999].bar").get == Seq(
      PathArray("foo", 9999),
      PathObjectOrField("bar")))
  }

  test("foo[2].bar[80].baz") {
    assert(JsonPathParser.parse("foo[2].bar[80].baz").get == Seq(
      PathArray("foo", 2),
      PathArray("bar", 80),
      PathObjectOrField("baz")))
  }

  test("bad idx") {
    assert(JsonPathParser.parse("foo[bad]").isFailure)
  }

  test("missing idx end") {
    assert(JsonPathParser.parse("foo[12").isFailure)
  }

  test("empty") {
    assert(JsonPathParser.parse("") == Success(Seq()))
  }

}
