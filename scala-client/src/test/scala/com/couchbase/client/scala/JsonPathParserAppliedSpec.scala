/*
 * Copyright (c) 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    val bytes: Array[Byte] = Conversions.encode[JsonObject](content).get
    ProjectionsApplier.parse(path, bytes).get
  }

  private def wrap(path: String, content: JsonArray): JsonObject = {
    val bytes: Array[Byte] = Conversions.encode[JsonArray](content).get
    ProjectionsApplier.parse(path, bytes).get
  }
  @Test
  def parse_string(): Unit = {
    assert(ProjectionsApplier.parseContent("hello".getBytes(CharsetUtil.UTF_8)).get == "hello")
  }

  @Test
  def parse_obj(): Unit = {
    val out = ProjectionsApplier
      .parseContent("""{"hello":"world"}""".getBytes(CharsetUtil.UTF_8))
      .get
      .asInstanceOf[JsonObject]
    assert(out.str("hello") == "world")
  }

  @Test
  def parse_arr(): Unit = {
    val out = ProjectionsApplier
      .parseContent("""["hello","world"]""".getBytes(CharsetUtil.UTF_8))
      .get
      .asInstanceOf[JsonArray]
    assert(out.toSeq == Seq("hello", "world"))
  }

  @Test
  def name(): Unit = {
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
  def age(): Unit = {
    val json = wrap("age", obj.num("age"))
    assert(json.num("age") == 26)
  }

  @Test
  def animals(): Unit = {
    val json = wrap("animals", obj.arr("animals"))
    val arr  = json.arr("animals")
    assert(arr.size == 3)
    assert(arr.toSeq == Seq("cat", "dog", "parrot"))
  }

  @Test
  def animals_0(): Unit = {
    val json = wrap("animals[0]", obj.arr("animals").str(0))
    val arr  = json.arr("animals")
    assert(arr.toSeq == Seq("cat"))
  }

  @Test
  def animals_2(): Unit = {
    val json = wrap("animals[2]", obj.arr("animals").str(2))
    val arr  = json.arr("animals")
    assert(arr.toSeq == Seq("parrot"))
  }

  @Test
  def addresses_0_address(): Unit = {
    val json = wrap("addresses[0].address", obj.arr("addresses").obj(0).str("address"))
    assert(json.arr("addresses").obj(0).str("address") == "123 Fake Street")
  }

  @Test
  def attributes(): Unit = {
    val json  = wrap("attributes", obj.obj("attributes"))
    val field = json.obj("attributes")
    assert(field.size == 3)
    assert(field.arr("hobbies").obj(1).str("type") == "summer sports")
    assert(field.safe.dyn.hobbies(1).name.str.get == "water skiing")
  }

  @Test
  def attributes_hair(): Unit = {
    val json = wrap("attributes.hair", obj.obj("attributes").str("hair"))
    assert(json.obj("attributes").str("hair") == "brown")
  }

  @Test
  def attributes_dimensions(): Unit = {
    val json = wrap("attributes.dimensions", obj.obj("attributes").obj("dimensions"))
    assert(json.obj("attributes").obj("dimensions").num("height") == 67)
    assert(json.obj("attributes").obj("dimensions").num("weight") == 175)
  }

  @Test
  def attributes_dimensions_height(): Unit = {
    val json =
      wrap("attributes.dimensions.height", obj.obj("attributes").obj("dimensions").num("height"))
    assert(json.obj("attributes").obj("dimensions").num("height") == 67)
  }

  @Test
  def attributes_hobbies_1_type(): Unit = {
    val json =
      wrap("attributes.hobbies[1].type", obj.obj("attributes").arr("hobbies").obj(1).str("type"))
    assert(json.obj("attributes").arr("hobbies").obj(0).str("type") == "summer sports")
  }
}
