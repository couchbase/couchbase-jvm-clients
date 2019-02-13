/*
 * Copyright (c) 2005, 2013, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */
package benchmarks.json

import java.util

import benchmarks.json.JsonObjectBench.{gen, using}
import com.couchbase.client.java.codec.{Decoder, DefaultDecoder, DefaultEncoder}
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.codec.Conversions.Encodable
//import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
//import com.github.plokhotnyuk.jsoniter_scala.macros.{CodecMakerConfig, JsonCodecMaker}
//import experiments.{JsoniterArray, JsoniterObject}
//import io.circe.Encoder
//import jsonobject.JsonObjectExperiment
//import play.api.libs.json.{JsNumber, JsObject, JsString, JsValue}
import org.scalameter.api._
//import org.typelevel.jawn.ast.{JNum, JObject, JString}
//import ujson.ujson.BytesRenderer

import scala.collection.mutable.ArrayBuffer


/**
  * In the red corner: JsonObject (Java edition)!  In the blue: JsonObject (Scala edition)!  Fight!
  */
object JsonObjectBench extends Bench.ForkedTime {
  val gen = Gen.unit("num")
  val content = Jackson.content


  override def reporter: Reporter[Double] = new SimpleLoggingReporter[Double]


  private val FieldAddress = "123 Fake Street"
  private val FieldName = "John Smith"

  performance of "Scala" in {
    import com.couchbase.client.scala.json._

    performance of "creating full" in {
      import com.couchbase.client.scala.json._

      using(gen) in {
        r =>
          val json = JsonObject.create
            .put("name", FieldName)
            .put("address", JsonArray.create.add(JsonObject.create.put("address", FieldAddress)))
            .put("age", 29)
      }
    }

    performance of "scalating object" in {
      using(gen) in {
        r =>
          val json = JsonObject.create
      }
    }

    performance of "creating object and putting two" in {
      using(gen) in {
        r =>
          val json = JsonObject.create
            .put("name", FieldName)
            .put("age", 29)
      }
    }

    performance of "just creating array" in {
      using(gen) in {
        r =>
          val json = JsonArray.create
      }
    }

    performance of "creating array and putting two" in {
      using(gen) in {
        r =>
          val json = JsonArray.create
            .add(FieldName).add(29)
      }
    }

    performance of "creating array from" in {
      using(gen) in {
        r =>
          val json = JsonArray(FieldName, FieldName)
      }
    }

    performance of "creating array from existing map" in {
      val map = Map("name" -> FieldName, "age" -> 29)

      using(gen) in {
        r =>
          val json = JsonArray(map)
      }
    }

  }

  performance of "Java" in {
    import com.couchbase.client.java.json._

    performance of "creating full" in {
      using(gen) in {
        r =>
          val json = JsonObject.create()
            .put("name", FieldName)
            .put("address", JsonArray.create.add(JsonObject.create.put("address", FieldAddress)))
            .put("age", 29)
      }
    }

    performance of "just creating object" in {
      using(gen) in {
        r =>
          val json = JsonObject.create()
      }
    }

    performance of "creating object and putting two" in {
      using(gen) in {
        r =>
          val json = JsonObject.create
            .put("name", FieldName)
            .put("age", 29)
      }
    }


    performance of "just creating array" in {
      using(gen) in {
        r =>
          val json = JsonArray.create
      }
    }

    performance of "creating array and putting two" in {
      using(gen) in {
        r =>
          val json = JsonArray.create
              .add(FieldName).add(29)
      }
    }

    performance of "creating array from" in {
      using(gen) in {
        r =>
          val json = JsonArray.from(FieldName, FieldName)
      }
    }

  }

  // Benchmarking the underlying data structures
  performance of "Data structures" in {
    performance of "Java HashMap" in {
      using(gen) in {
        r =>
          val map = new util.HashMap[String, Object]()
          map.put("hello", "world")
          map.put("foo", "bar")
          map.put("age", "22")
      }
    }

    performance of "Scala mutable Map" in {
      using(gen) in {
        r =>
          val map = collection.mutable.Map.empty[String, Any]
          map.put("hello", "world")
          map.put("foo", "bar")
          map.put("age", "22")
      }
    }


    performance of "Scala mutable AnyRefMap" in {
      using(gen) in {
        r =>
          val map = collection.mutable.AnyRefMap.empty[String, Any]
          map.put("hello", "world")
          map.put("foo", "bar")
          map.put("age", "22")
      }
    }

    performance of "Scala immutable Map" in {
      using(gen) in {
        r =>
          val map = collection.immutable.Map[String, Any]("hello" -> "world",
            "foo" -> "bar",
            "age" -> "22")
      }
    }
  }

}
