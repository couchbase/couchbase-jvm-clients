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

import com.couchbase.client.java.codec.{Decoder, DefaultDecoder, DefaultEncoder}
import com.couchbase.client.java.kv.EncodedDocument
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.codec.Conversions.Encodable
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.{CodecMakerConfig, JsonCodecMaker}
import experiments.JsoniterObject
import io.circe.Encoder
import io.netty.util.CharsetUtil
import jsonobject.JsonObjectExperiment
import org.scalameter
import org.scalameter.CurveData
import play.api.libs.json.{JsNumber, JsObject, JsString, JsValue}
import org.scalameter.api._
import org.scalameter.utils.Tree
import org.typelevel.jawn.ast.{JNum, JObject, JString}
import ujson.BytesRenderer


/** Simply logs the measurement data to the standard output.
  */
case class SimpleLoggingReporter[T]() extends Reporter[T] {

  def report(result: CurveData[T], persistor: Persistor) {
    // output context

    val time = result.measurements(0).value + result.measurements(0).units

    val name = result.context.scope.toString
    scalameter.log(f"$name%-60s ${time}")
    //    val machineKeys = result.context.properties
    //      .filterKeys(Context.machine.properties.keySet.contains).toSeq.sortBy(_._1)
    //    for ((key, value) <- machineKeys) {
    //      scalameter.log(s"$key: $value")
    //    }

    // output measurements
    //    for (measurement <- result.measurements) {
    //      scalameter.log(s"${measurement.params}: ${measurement.value}")
    //    }

    // add a new line
    //    log("")
  }

  def report(result: Tree[CurveData[T]], persistor: Persistor) = true
}

case class Address(address: String)

case class User(name: String, age: Int, addresses: Seq[Address])
case class User2(name: String, age: Int)

object Address {
  implicit val rw: upickle.default.ReadWriter[Address] = upickle.default.macroRW
  implicit val codec: JsonValueCodec[Address] = JsonCodecMaker.make[Address](CodecMakerConfig())
  implicit val decoder: io.circe.Decoder[Address] = io.circe.generic.semiauto.deriveDecoder[Address]
  implicit val encoder: io.circe.Encoder[Address] = io.circe.generic.semiauto.deriveEncoder[Address]
}

object User {
  implicit val rw: upickle.default.ReadWriter[User] = upickle.default.macroRW
  implicit val codec: JsonValueCodec[User] = JsonCodecMaker.make[User](CodecMakerConfig())
  implicit val decoder: io.circe.Decoder[User] = io.circe.generic.semiauto.deriveDecoder[User]
  implicit val encoder: io.circe.Encoder[User] = io.circe.generic.semiauto.deriveEncoder[User]
}

// Jackson gets unhappy about being run directly inside the scalameter closures, for reasons know only to itself
object Jackson {
  def read(encoded: Array[Byte], value: Class[_]) = {
    mapper.readValue(encoded, value)
  }

  val content = User("John Smith", 29, List(Address("123 Fake Street")))
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def jacksonToBytes() = {
    val bytes: Array[Byte] = mapper.writeValueAsBytes(Jackson.content)
    bytes

  }

  val json = "{\"hello\":\"world\",\"foo\":\"bar\",\"age\":22}"
  val encoded = json.getBytes(CharsetUtil.UTF_8)

  val doc = new EncodedDocument(0, encoded)
}

// Change this to LocalTime for a fast result
object Creating extends Bench.ForkedTime {
  val gen = Gen.unit("num")
  val content = Jackson.content


  override def reporter: Reporter[Double] = new SimpleLoggingReporter[Double]


  performance of "Decoding byte array to JSON AST" in {
    performance of "JsonObject (Java)" in {
      import com.couchbase.client.java.json.JsonObject

      using(gen) in {
        r => {
          val decoded = DefaultDecoder.INSTANCE.asInstanceOf[Decoder[JsonObject]].decode(classOf[JsonObject], Jackson.doc)
          val hello = decoded.getString("hello")
          val age = decoded.getInt("age")

        }
      }
    }

    performance of "JsonObject (Scala)" in {

      using(gen) in {
        r => {
          val decoded = Conversions.decode(Jackson.encoded)(jsonobject.Decoders.JsonObjectExperiment).get
          val hello = decoded.getString("hello")
          val age = decoded.getInt("age")
        }
      }
    }

    performance of "JsoniterObject (Scala)" in {

      using(gen) in {
        r => {
          val decoded = Conversions.decode(Jackson.encoded)(jsonobject.Decoders.JsoniterObjectConvert).get
          val hello = decoded.getString("hello")
          val age = decoded.getInt("age")
        }
      }
    }

    performance of "Play" in {
      using(gen) in {
        r => {
          val decoded = Conversions.decode[play.api.libs.json.JsValue](Jackson.encoded).get.asInstanceOf[JsObject]
          // Some parsers do lazy decoding, so read a subset of fields to get a fairer comparison
          val hello = decoded("hello").as[String]
          val age = decoded("age").as[Int]
        }
      }
    }

    performance of "Jawn" in {
      import org.typelevel.jawn.ast._


      using(gen) in {
        r => {
          val decoded = Conversions.decode[JValue](Jackson.encoded).get.asInstanceOf[JObject]
          val hello = decoded.get("hello").asString
          val age = decoded.get("age").asInt
        }
      }
    }

    performance of "Json4s" in {
      val json = play.api.libs.json.Json.obj("hello" -> "world",
        "foo" -> "bar",
        "age" -> 22)
      val encoded: Array[Byte] = Conversions.encode(json).get._1

      using(gen) in {
        r => {
          val decoded = Conversions.decode[org.json4s.JsonAST.JValue](encoded).get.asInstanceOf[org.json4s.JsonAST.JValue]
          // Not clear how to pull fields out
          //          val hello: String = org.json4s.render(decoded \ "hello")
        }
      }
    }

    performance of "upickle" in {
      val json = ujson.Obj("hello" -> "world", "foo" -> "bar", "age" -> 22)
      val encoded: Array[Byte] = Conversions.encode(json).get._1

      using(gen) in {
        r => {
          val decoded = Conversions.decode[ujson.Obj](encoded).get
          val hello = decoded("hello").str
          val age = decoded("age").num
        }
      }
    }

  }


  performance of "Just creating JSON AST" in {
    performance of "upickle" in {
      using(gen) in {
        r => val json = ujson.Obj("hello" -> "world", "foo" -> "bar", "age" -> 22)
      }
    }

    performance of "upickle (alternative1)" in {
      using(gen) in {
        r => val json = ujson.Obj.from(Seq("hello" -> "world", "foo" -> "bar", "age" -> 22))
      }
    }


    performance of "upickle (alternative2)" in {
      using(gen) in {
        r => {
          val json = ujson.Obj()
          json("hello") = "world"
          json("foo") = "bar"
          json("age") = 22
        }
      }
    }

    performance of "JsonObject (Java)" in {
      using(gen) in {
        r =>
          val json = com.couchbase.client.java.json.JsonObject.create()
            .put("hello", "world")
            .put("foo", "bar")
            .put("age", 22)
      }
    }

    performance of "JsonObject (Scala)" in {
      using(gen) in {
        r =>
          val json = jsonobject.JsonObjectExperiment.create
            .put("hello", "world")
            .put("foo", "bar")
            .put("age", 22)
      }
    }
  }


  performance of "Encoding case class to byte array" in {


    performance of "upickle (fast)" in {
      using(gen) in {
        r => {
          val bytes: Array[Byte] = upickle.default.transform(content).to(BytesRenderer()).toBytes
          val encoded: Array[Byte] = Conversions.encode(bytes)(Encodable.AsJson.BytesConvert).get._1
        }
      }
    }

    performance of "jsoniter (fast)" in {
      using(gen) in {
        r => {
          val bytes: Array[Byte] = com.github.plokhotnyuk.jsoniter_scala.core.writeToArray(content)
          val encoded: Array[Byte] = Conversions.encode(bytes)(Encodable.AsJson.BytesConvert).get._1
        }
      }
    }



    performance of "jackson bytes (fast)" in {
      using(gen) in {
        r => {
          val bytes: Array[Byte] = Jackson.jacksonToBytes()
          val encoded: Array[Byte] = Conversions.encode(bytes)(Encodable.AsJson.BytesConvert).get._1
        }
      }
    }


    performance of "jackson string (fast)" in {
      using(gen) in {
        r => {
          val json: String = Jackson.mapper.writeValueAsString(content)
          val encoded: Array[Byte] = Conversions.encode(json)(Encodable.AsJson.StringConvert).get._1
        }
      }
    }

    performance of "upickle (default)" in {
      using(gen) in {
        r => {
          val bytes: Array[Byte] = upickle.default.transform(content).to(BytesRenderer()).toBytes
          val encoded: Array[Byte] = Conversions.encode(bytes).get._1
        }
      }
    }

    performance of "jsoniter (default)" in {
      using(gen) in {
        r => {
          val bytes: Array[Byte] = com.github.plokhotnyuk.jsoniter_scala.core.writeToArray(content)
          val encoded: Array[Byte] = Conversions.encode(bytes).get._1
        }
      }
    }


    performance of "jackson bytes (default)" in {
      using(gen) in {
        r => {
          val bytes: Array[Byte] = Jackson.mapper.writeValueAsBytes(content)
          val encoded: Array[Byte] = Conversions.encode(bytes).get._1
        }
      }
    }

    performance of "jackson string (default)" in {
      using(gen) in {
        r => {
          val json: String = Jackson.mapper.writeValueAsString(content)
          val encoded: Array[Byte] = Conversions.encode(json).get._1
        }
      }
    }

    //        performance of "circe" in {
    //                  import io.circe.syntax._
    //
    //          using(gen) in {
    //            r => {
    //              val json: io.circe.Json  = content.asJson
    //              val encoded: Array[Byte] = Conversions.encode(json).get._1
    //            }
    //          }
    //        }


  }

  performance of "Decoding case class from byte array" in {


    val origContent = User("John Smith", 29, List(Address("123 Fake Street")))
    val encodedBytes: Array[Byte] = upickle.default.transform(origContent).to(BytesRenderer()).toBytes
    val encoded: Array[Byte] = Conversions.encode(encodedBytes).get._1

    performance of "upickle" in {
      using(gen) in {
        r => {
          val user = upickle.default.read[User](encoded)
        }
      }
    }

    performance of "jsoniter" in {
      using(gen) in {
        r => {
          val user = com.github.plokhotnyuk.jsoniter_scala.core.readFromArray[User](encoded)
        }
      }
    }

    performance of "jackson" in {
      using(gen) in {
        r => {
          val user = Jackson.read(encoded, classOf[User])
        }
      }
    }

    //    performance of "circe" in {
    //      using(gen) in {
    //        r => {
    //          val json = new String(encoded, CharsetUtil.UTF_8)
    //          println(json)
    //          val user = io.circe.parser.decode[User](json)
    //        }
    //      }
    //    }

  }

  performance of "Encoding JSON AST to byte array" in {

    performance of "upickle" in {
      using(gen) in {
        r => {
          val json = ujson.Obj("hello" -> "world", "foo" -> "bar", "age" -> 22)
          val encoded: Array[Byte] = Conversions.encode(json).get._1
        }
      }
    }

    performance of "JsonObject (Java)" in {
      using(gen) in {
        r => {
          val json = com.couchbase.client.java.json.JsonObject.create()
            .put("hello", "world")
            .put("foo", "bar")
            .put("age", 22)
          val encoded: Array[Byte] = DefaultEncoder.INSTANCE.encode(json).content()
        }
      }
    }

    performance of "JsonObject (Scala)" in {
      using(gen) in {
        r => {
          val json = jsonobject.JsonObjectExperiment.create
            .put("hello", "world")
            .put("foo", "bar")
            .put("age", 22)
          val encoded: Array[Byte] = Conversions.encode(json)(jsonobject.Encoders.JsonObjectExperimentConvert).get._1
        }
      }
    }

    performance of "JsoniterObject (Scala)" in {
      using(gen) in {
        r => {
          val json = JsoniterObject.create
            .put("hello", "world")
            .put("foo", "bar")
            .put("age", 22)
          val encoded: Array[Byte] = Conversions.encode(json)(jsonobject.Encoders.JsoniterObjectConvert).get._1
        }
      }
    }

    performance of "Play" in {
      using(gen) in {
        r => {
          val json = play.api.libs.json.Json.obj("hello" -> "world",
            "foo" -> "bar",
            "age" -> 22)
          val encoded: Array[Byte] = Conversions.encode(json).get._1
        }
      }
    }

    performance of "Jawn" in {
      import org.typelevel.jawn.ast._

      using(gen) in {
        r => {
          val json = JObject.fromSeq(Seq("hello" -> JString("world"),
            "foo" -> JString("bar"),
            "age" -> JNum(22)))
          val encoded: Array[Byte] = Conversions.encode(json).get._1
        }
      }
    }
  }

}
