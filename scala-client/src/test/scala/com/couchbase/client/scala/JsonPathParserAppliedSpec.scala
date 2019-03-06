package com.couchbase.client.scala

import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.json.{JsonArray, JsonObject, PathArray, PathObjectOrField}
import com.couchbase.client.scala.kv.{JsonPathParser, ProjectionsApplier}
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import org.scalatest.FunSuite

import scala.util.Success

class ProjectionsApplierSpec extends FunSuite {
  private def wrap(path: String, content: String): JsonObject = {
    ProjectionsApplier.parse(path, content.getBytes(CharsetUtil.UTF_8)).get
  }

  private def wrap(path: String, content: Int): JsonObject = {
    ProjectionsApplier.parse(path, content.toString.getBytes(CharsetUtil.UTF_8)).get
  }

  private def wrap(path: String, content: JsonObject): JsonObject = {
    val bytes: Array[Byte] = Conversions.encode[JsonObject](content).get._1
    ProjectionsApplier.parse(path, bytes).get
  }

  private def wrap(path: String, content: JsonArray): JsonObject = {
    val bytes: Array[Byte] = Conversions.encode[JsonArray](content).get._1
    ProjectionsApplier.parse(path, bytes).get
  }



  test("parse string") {
    assert(ProjectionsApplier.parseContent("hello".getBytes(CharsetUtil.UTF_8)).get == "hello")
  }

  test("parse obj") {
    val out = ProjectionsApplier.parseContent("""{"hello":"world"}""".getBytes(CharsetUtil.UTF_8)).get.asInstanceOf[JsonObject]
    assert(out.str("hello") == "world")
  }

  test("parse arr") {
    val out = ProjectionsApplier.parseContent("""["hello","world"]""".getBytes(CharsetUtil.UTF_8)).get.asInstanceOf[JsonArray]
    assert(out.toSeq == Seq("hello","world"))
  }

  test("name") {
    val out = wrap("name", "Emmy-lou Dickerson")
    assert(out.str("name") == "Emmy-lou Dickerson")
  }

  val raw = """{
              |    "name": "Emmy-lou Dickerson",
              |    "age": 26,
              |    "animals": ["cat", "dog", "parrot"],
              |    "addresses": [{"address":"123 Fake Street"}],
              |    "attributes": {
              |        "hair": "brown",
              |        "dimensions": {
              |            "height": 67,
              |            "weight": 175
              |        },
              |        "hobbies": [{
              |                "type": "winter sports",
              |                "name": "curling"
              |            },
              |            {
              |                "type": "summer sports",
              |                "name": "water skiing",
              |                "details": {
              |                    "location": {
              |                        "lat": 49.282730,
              |                        "long": -123.120735
              |                    }
              |                }
              |            }
              |        ]
              |    }
              |}
              |""".stripMargin
  val obj = JsonObject.fromJson(raw)


  test("age") {
    val json = wrap("age", obj.num("age"))
    assert(json.num("age") == 26)
  }

  test("animals") {
    val json = wrap("animals", obj.arr("animals"))
    val arr = json.arr("animals")
    assert(arr.size == 3)
    assert(arr.toSeq == Seq("cat", "dog", "parrot"))
  }

  test("animals[0]") {
    val json = wrap("animals[0]", obj.arr("animals").str(0))
    val arr = json.arr("animals")
    assert(arr.toSeq == Seq("cat"))
  }

  test("animals[2]") {
    val json = wrap("animals[2]", obj.arr("animals").str(2))
    val arr = json.arr("animals")
    assert(arr.toSeq == Seq("parrot"))
  }

  test("addresses[0].address") {
    val json = wrap("addresses[0].address", obj.arr("addresses").obj(0).str("address"))
    assert(json.arr("addresses").obj(0).str("address") == "123 Fake Street")
  }

  test("attributes") {
    val json = wrap("attributes", obj.obj("attributes"))
    val field = json.obj("attributes")
    assert(field.size == 3)
    assert(field.arr("hobbies").obj(1).str("type") == "summer sports")
    assert(field.safe.dyn.hobbies(1).name.str.get == "water skiing")
  }

  test("attributes.hair") {
    val json = wrap("attributes.hair", obj.obj("attributes").str("hair"))
    assert(json.obj("attributes").str("hair") == "brown")
  }

  test("attributes.dimensions") {
    val json = wrap("attributes.dimensions", obj.obj("attributes").obj("dimensions"))
    assert(json.obj("attributes").obj("dimensions").num("height") == 67)
    assert(json.obj("attributes").obj("dimensions").num("weight") == 175)
  }

  test("attributes.dimensions.height") {
    val json = wrap("attributes.dimensions.height", obj.obj("attributes").obj("dimensions").num("height"))
    assert(json.obj("attributes").obj("dimensions").num("height") == 67)
  }

  test("attributes.hobbies[1].type") {
    val json = wrap("attributes.hobbies[1].type", obj.obj("attributes").arr("hobbies").obj(1).str("type"))
    assert(json.obj("attributes").arr("hobbies").obj(0).str("type") == "summer sports")
  }
}
