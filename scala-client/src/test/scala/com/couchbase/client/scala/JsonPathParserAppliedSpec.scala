package com.couchbase.client.scala

import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.json.{JsonArray, JsonObject}
import com.couchbase.client.scala.kv.ProjectionsApplier
import org.junit.jupiter.api.Test

class ProjectionsApplierSpec {
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


  @Test
  def parse_string() {
    assert(ProjectionsApplier.parseContent("hello".getBytes(CharsetUtil.UTF_8)).get == "hello")
  }

  @Test
  def parse_obj() {
    val out = ProjectionsApplier.parseContent("""{"hello":"world"}""".getBytes(CharsetUtil.UTF_8)).get
      .asInstanceOf[JsonObject]
    assert(out.str("hello") == "world")
  }

  @Test
  def parse_arr() {
    val out = ProjectionsApplier.parseContent("""["hello","world"]""".getBytes(CharsetUtil.UTF_8)).get
      .asInstanceOf[JsonArray]
    assert(out.toSeq == Seq("hello", "world"))
  }

  @Test
  def name() {
    val out = wrap("name", "Emmy-lou Dickerson")
    assert(out.str("name") == "Emmy-lou Dickerson")
  }

  val raw =
    """{
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


  @Test
  def age() {
    val json = wrap("age", obj.num("age"))
    assert(json.num("age") == 26)
  }

  @Test
  def animals() {
    val json = wrap("animals", obj.arr("animals"))
    val arr = json.arr("animals")
    assert(arr.size == 3)
    assert(arr.toSeq == Seq("cat", "dog", "parrot"))
  }

  @Test
  def animals_0() {
    val json = wrap("animals[0]", obj.arr("animals").str(0))
    val arr = json.arr("animals")
    assert(arr.toSeq == Seq("cat"))
  }

  @Test
  def animals_2() {
    val json = wrap("animals[2]", obj.arr("animals").str(2))
    val arr = json.arr("animals")
    assert(arr.toSeq == Seq("parrot"))
  }

  @Test
  def addresses_0_address() {
    val json = wrap("addresses[0].address", obj.arr("addresses").obj(0).str("address"))
    assert(json.arr("addresses").obj(0).str("address") == "123 Fake Street")
  }

  @Test
  def attributes() {
    val json = wrap("attributes", obj.obj("attributes"))
    val field = json.obj("attributes")
    assert(field.size == 3)
    assert(field.arr("hobbies").obj(1).str("type") == "summer sports")
    assert(field.safe.dyn.hobbies(1).name.str.get == "water skiing")
  }

  @Test
  def attributes_hair() {
    val json = wrap("attributes.hair", obj.obj("attributes").str("hair"))
    assert(json.obj("attributes").str("hair") == "brown")
  }

  @Test
  def attributes_dimensions() {
    val json = wrap("attributes.dimensions", obj.obj("attributes").obj("dimensions"))
    assert(json.obj("attributes").obj("dimensions").num("height") == 67)
    assert(json.obj("attributes").obj("dimensions").num("weight") == 175)
  }

  @Test
  def attributes_dimensions_height() {
    val json = wrap("attributes.dimensions.height", obj.obj("attributes").obj("dimensions").num("height"))
    assert(json.obj("attributes").obj("dimensions").num("height") == 67)
  }

  @Test
  def attributes_hobbies_1_type() {
    val json = wrap("attributes.hobbies[1].type", obj.obj("attributes").arr("hobbies").obj(1).str("type"))
    assert(json.obj("attributes").arr("hobbies").obj(0).str("type") == "summer sports")
  }
}
