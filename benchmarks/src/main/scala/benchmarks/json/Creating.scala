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
import com.couchbase.client.java.json.JsonObject
import com.couchbase.client.java.kv.EncodedDocument
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.codec.Conversions.Encodable
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.{CodecMakerConfig, JsonCodecMaker}
import io.circe.Encoder
import io.netty.util.CharsetUtil
import org.scalameter
import org.scalameter.CurveData
import play.api.libs.json.{JsNumber, JsObject, JsString, JsValue}
import org.scalameter.api._
import org.scalameter.utils.Tree
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

case class User(name: String, age: Int, addresses: List[Address])

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


object Creating extends Bench.LocalTime {
  val gen = Gen.unit("num")

  override def reporter: Reporter[Double] = new SimpleLoggingReporter[Double]

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

    performance of "JsonObject" in {
      using(gen) in {
        r =>
          val json = JsonObject.create()
            .put("hello", "world")
            .put("foo", "bar")
            .put("age", 22)
      }
    }
  }


  performance of "Encoding case class to byte array" in {


    val content = User("John Smith", 29, List(Address("123 Fake Street")))

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

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)


    performance of "jackson bytes (fast)" in {
      using(gen) in {
        r => {
          val bytes: Array[Byte] = mapper.writeValueAsBytes(content)
          val encoded: Array[Byte] = Conversions.encode(bytes)(Encodable.AsJson.BytesConvert).get._1
        }
      }
    }


    performance of "jackson string (fast)" in {
      using(gen) in {
        r => {
          val json: String = mapper.writeValueAsString(content)
          val encoded: Array[Byte] = Conversions.encode(json)(Encodable.AsJson.StringConvert).get._1
        }
      }
    }

    performance of "upickle (safe)" in {
      using(gen) in {
        r => {
          val bytes: Array[Byte] = upickle.default.transform(content).to(BytesRenderer()).toBytes
          val encoded: Array[Byte] = Conversions.encode(bytes).get._1
        }
      }
    }

    performance of "jsoniter (safe)" in {
      using(gen) in {
        r => {
          val bytes: Array[Byte] = com.github.plokhotnyuk.jsoniter_scala.core.writeToArray(content)
          val encoded: Array[Byte] = Conversions.encode(bytes).get._1
        }
      }
    }


    performance of "jackson bytes (safe)" in {
      using(gen) in {
        r => {
          val bytes: Array[Byte] = mapper.writeValueAsBytes(content)
          val encoded: Array[Byte] = Conversions.encode(bytes).get._1
        }
      }
    }

    performance of "jackson string (safe)" in {
      using(gen) in {
        r => {
          val json: String = mapper.writeValueAsString(content)
          val encoded: Array[Byte] = Conversions.encode(json).get._1
        }
      }
    }

    //    performance of "circe" in {
    //              import io.circe.syntax._
    //
    //      using(gen) in {
    //        r => {
    //          val json: io.circe.Json  = content.asJson
    //          val encoded: Array[Byte] = Conversions.encode(json).get._1
    //        }
    //      }
    //    }


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

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    performance of "jackson" in {
      using(gen) in {
        r => {
          val user = mapper.readValue(encoded, classOf[User])
        }
      }
    }

    performance of "circe" in {
      using(gen) in {
        r => {
          val json = new String(encoded, CharsetUtil.UTF_8)
          val user = io.circe.parser.decode[User](json)
        }
      }
    }

  }

  performance of "Creating JSON AST and encoding to byte array" in {

    performance of "upickle" in {
      using(gen) in {
        r => {
          val json = ujson.Obj("hello" -> "world", "foo" -> "bar", "age" -> 22)
          val encoded: Array[Byte] = Conversions.encode(json).get._1
        }
      }
    }

    performance of "JsonObject" in {
      using(gen) in {
        r => {
          val json = JsonObject.create()
            .put("hello", "world")
            .put("foo", "bar")
            .put("age", 22)
          val encoded: Array[Byte] = DefaultEncoder.INSTANCE.encode(json).content()
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

    performance of "Json4s" in {
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

  performance of "Decoding byte array to JSON AST" in {
    performance of "Play" in {
      val json = play.api.libs.json.Json.obj("hello" -> "world",
        "foo" -> "bar",
        "age" -> 22)
      val encoded: Array[Byte] = Conversions.encode(json).get._1

      using(gen) in {
        r => {
          Conversions.decode[play.api.libs.json.JsValue](encoded).get.asInstanceOf[JsObject]
        }
      }
    }

        performance of "JsonObject" in {

          val json = "{\"hello\":\"world\", \"foo\":\"bar\", \"age\": 22}"
          val encoded = DefaultEncoder.INSTANCE.encode(json).content()
          val doc = new EncodedDocument(0, encoded)

    //        def contentAs[T](target: Class[T]): T = contentAs(target, DefaultDecoder.INSTANCE.asInstanceOf[Decoder[T]])
    //
    //        def contentAs[T](target: Class[T], decoder: Decoder[T]): T = decoder.decode(target, doc)


          using(gen) in {
            r => {
              DefaultDecoder.INSTANCE.asInstanceOf[Decoder[JsonObject]].decode(classOf[JsonObject], doc)
    //          val decoded = contentAs(classOf[JsonObject])
            }
          }
        }

    performance of "Jawn" in {
      import org.typelevel.jawn.ast._

      val json = JObject.fromSeq(Seq("hello" -> JString("world"),
        "foo" -> JString("bar"),
        "age" -> JNum(22)))
      val encoded: Array[Byte] = Conversions.encode(json).get._1

      using(gen) in {
        r => Conversions.decode[JValue](encoded).get.asInstanceOf[JObject]
      }
    }

    performance of "Json4s" in {
      val json = play.api.libs.json.Json.obj("hello" -> "world",
        "foo" -> "bar",
        "age" -> 22)
      val encoded: Array[Byte] = Conversions.encode(json).get._1

      using(gen) in {
        r => Conversions.decode[org.json4s.JsonAST.JValue](encoded).get.asInstanceOf[org.json4s.JsonAST.JValue]
      }
    }

    performance of "upickle" in {
      val json = ujson.Obj("hello" -> "world", "foo" -> "bar", "age" -> 22)
      val encoded: Array[Byte] = Conversions.encode(json).get._1

      using(gen) in {
        r => Conversions.decode[ujson.Obj](encoded).get
      }
    }


    //  @Benchmark def upickleScala(): Unit = {
    //    val decoded: ujson.Obj = Conversions.decode[ujson.Obj](Decoding.encoded).get
    //  }
    //
    //
    //  @Benchmark def playScala(): Unit = {
    //  }
    //

  }
}
