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
//import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
//import com.github.plokhotnyuk.jsoniter_scala.macros.{CodecMakerConfig, JsonCodecMaker}
//import experiments.{JsoniterArray, JsoniterObject}
//import io.circe.Encoder
import io.netty.util.CharsetUtil
//import jsonobject.JsonObjectExperiment
import org.scalameter
import org.scalameter.CurveData
//import play.api.libs.json.{JsNumber, JsObject, JsString, JsValue}
import org.scalameter.api._
import org.scalameter.utils.Tree
//import org.typelevel.jawn.ast.{JNum, JObject, JString}
//import ujson.ujson.BytesRenderer

import scala.collection.mutable.ArrayBuffer


/** Simply logs the measurement data to the standard output.
  */
case class SimpleLoggingReporter[T]() extends Reporter[T] {

  def report(result: CurveData[T], persistor: Persistor) {
    // Multiply and int purely to get more readable results.  Absolutely values not that important after all
    val time: Int = (result.measurements(0).value.toString.toDouble * 10000).toInt
//    val time = result.measurements(0).value + result.measurements(0).units

    val name = result.context.scope.toString
    scalameter.log(f"$name%-60s $time%5s")
  }

  def report(result: Tree[CurveData[T]], persistor: Persistor) = true
}

case class Address(address: String)

case class User(name: String, age: Int, addresses: Seq[Address])
case class User2(name: String, age: Int)

object Address {
  implicit val rw: upickle.default.ReadWriter[Address] = upickle.default.macroRW
  implicit val codec: com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec[Address] = 
    com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker.make[Address](com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig())
  implicit val decoder: io.circe.Decoder[Address] = io.circe.generic.semiauto.deriveDecoder[Address]
  implicit val encoder: io.circe.Encoder[Address] = io.circe.generic.semiauto.deriveEncoder[Address]
}

object User {
  implicit val rw: upickle.default.ReadWriter[User] = upickle.default.macroRW
  implicit val codec: com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec[User] = 
    com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker.make[User](com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig())
  implicit val decoder: io.circe.Decoder[User] = io.circe.generic.semiauto.deriveDecoder[User]
  implicit val encoder: io.circe.Encoder[User] = io.circe.generic.semiauto.deriveEncoder[User]
}

// Jackson gets unhappy about being run directly inside the scalameter closures, for reasons know only to itself
object Jackson {
  def read(encoded: Array[Byte], value: Class[_]) = {
    mapper.readValue(encoded, value)
  }

  val content = User("John Smith", 29, List(Address("123 Fake Street")))
  val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
  mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)

  def jacksonToBytes() = {
    val bytes: Array[Byte] = mapper.writeValueAsBytes(Jackson.content)
    bytes

  }

  val json = """{"name":"John Smith","age":29,"address":[{"address":"123 Fake Street"}]}"""
  val encoded = json.getBytes(CharsetUtil.UTF_8)

  val doc = new EncodedDocument(0, encoded)
}

// Change this to LocalTime for a fast result
object JsonBench extends Bench.LocalTime {
  val gen = Gen.unit("num")
  val content = Jackson.content


  override def reporter: Reporter[Double] = new SimpleLoggingReporter[Double]


  private val FieldAddress = "123 Fake Street"
  private val FieldName = "John Smith"
  
  performance of "Decoding byte array to JSON AST" in {
    performance of "JsonObject (Java)" in {
      import com.couchbase.client.java.json.JsonObject

      using(gen) in {
        r => {
          val decoded = DefaultDecoder.INSTANCE.asInstanceOf[Decoder[JsonObject]].decode(classOf[JsonObject], Jackson.doc)
          val hello = decoded.getString("name")
          val age = decoded.getInt("age")

        }
      }
    }

    performance of "JsonObject (Scala)" in {

      using(gen) in {
        r => {
          val decoded = Conversions.decode(Jackson.encoded)(jsonobject.Decoders.JsonObjectExperiment).get
          val hello = decoded.getString("name")
          val age = decoded.getInt("age")
        }
      }
    }

    performance of "JsoniterObject (Scala)" in {

      using(gen) in {
        r => {
          val decoded = Conversions.decode(Jackson.encoded)(jsonobject.Decoders.JsoniterObjectConvert).get
          val hello = decoded.getString("name")
          val age = decoded.getInt("age")
        }
      }
    }

    performance of "Play" in {
      import play.api.libs.json._
      
      using(gen) in {
        r => {
          val decoded = Conversions.decode[JsValue](Jackson.encoded).get.asInstanceOf[JsObject]
          // Some parsers do lazy decoding, so read a subset of fields to get a fairer comparison
          val hello = decoded("name").as[String]
          val age = decoded("age").as[Int]
        }
      }
    }

    performance of "Jawn" in {
      import org.typelevel.jawn.ast._


      using(gen) in {
        r => {
          val decoded = Conversions.decode[JValue](Jackson.encoded).get.asInstanceOf[JObject]
          val hello = decoded.get("name").asString
          val age = decoded.get("age").asInt
        }
      }
    }

    performance of "Json4s" in {
      val encoded: Array[Byte] = {
        import play.api.libs.json.Json._
        val json = obj("name" -> FieldName,
          "address" -> arr(obj("address" -> FieldAddress)),
          "age" -> 29)
        Conversions.encode(json).get._1
      }

      import org.json4s.JsonAST._

      using(gen) in {
        r => {
          val decoded = Conversions.decode[JValue](encoded).get.asInstanceOf[org.json4s.JsonAST.JValue]
          val JString(name) = decoded \ "name"
          val JInt(age) = decoded \ "age"
        }
      }
    }

    performance of "upickle" in {
      val json = ujson.Obj("name" -> FieldName, "address" -> ujson.Arr(ujson.Obj("address" -> FieldAddress)), "age" -> 29)
      val encoded: Array[Byte] = Conversions.encode(json).get._1

      using(gen) in {
        r => {
          val decoded = Conversions.decode[ujson.Obj](encoded).get
          val hello = decoded("name").str
          val age = decoded("age").num
        }
      }
    }
  }

  performance of "Just creating JSON AST" in {
    performance of "upickle" in {
      using(gen) in {
            // Found this is fastest of the ujson.Obj methods
            // ujson.Obj.from(Seq(...
            // and ujson.Obj(); json("name") =
        r => val json = ujson.Obj("name" -> FieldName, "address" -> ujson.Arr(ujson.Obj("address" -> FieldAddress)), "age" -> 29)
      }
    }

    performance of "play" in {
      import play.api.libs.json.Json.{obj, arr}

      using(gen) in {
        r =>  val json = obj("name" -> FieldName,
          "age" -> 29,
          "address" -> arr(obj("address" -> FieldAddress)))
      }
    }

    performance of "jawn" in {
      import org.typelevel.jawn.ast._

      using(gen) in {
        r => val json = JObject.fromSeq(Seq("name" -> JString(FieldName),
          "address" -> JArray(Array(JObject.fromSeq(Seq("address" -> JString(FieldAddress))))),
          "age" -> JNum(29)))
      }
    }
    
    performance of "JsonObject (Java)" in {
      import com.couchbase.client.java.json._

      using(gen) in {
        r =>
          val json = JsonObject.create()
            .put("name", FieldName)
            .put("address", JsonArray.from(JsonObject.create().put("address", FieldAddress)))
            .put("age", 29)
      }
    }

    performance of "JsonObject (Scala)" in {
      import com.couchbase.client.scala.json._

      using(gen) in {
        r =>
          val json = JsonObject.create
            .put("name", FieldName)
            .put("address", JsonArray(JsonObject.create.put("address", FieldAddress)))
            .put("age", 29)
      }
    }
  }


  performance of "Encoding case class to byte array" in {


    performance of "upickle (fast mode)" in {
      using(gen) in {
        r => {
          val bytes: Array[Byte] = upickle.default.transform(content).to(ujson.BytesRenderer()).toBytes
          val encoded: Array[Byte] = Conversions.encode(bytes)(Encodable.AsJson.BytesConvert).get._1
        }
      }
    }

    performance of "jsoniter (fast mode)" in {
      using(gen) in {
        r => {
          val bytes: Array[Byte] = com.github.plokhotnyuk.jsoniter_scala.core.writeToArray(content)
          val encoded: Array[Byte] = Conversions.encode(bytes)(Encodable.AsJson.BytesConvert).get._1
        }
      }
    }



    performance of "jackson bytes (fast mode)" in {
      using(gen) in {
        r => {
          val bytes: Array[Byte] = Jackson.jacksonToBytes()
          val encoded: Array[Byte] = Conversions.encode(bytes)(Encodable.AsJson.BytesConvert).get._1
        }
      }
    }


    performance of "jackson string (fast mode)" in {
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
          val bytes: Array[Byte] = upickle.default.transform(content).to(ujson.BytesRenderer()).toBytes
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


    val origContent = User(FieldName, 29, List(Address(FieldAddress)))
    val encodedBytes: Array[Byte] = upickle.default.transform(origContent).to(ujson.BytesRenderer()).toBytes
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
          val json = ujson.Obj("name" -> FieldName,
            "age" -> 29,
            "address" -> ujson.Arr(ujson.Obj("address" -> FieldAddress)))
          val encoded: Array[Byte] = Conversions.encode(json).get._1
        }
      }
    }

    performance of "JsonObject (Java)" in {
      import com.couchbase.client.java.json.{JsonArray, JsonObject}
      using(gen) in {
        r => {
          val json = JsonObject.create()
            .put("name", FieldName)
            .put("address", JsonArray.from(JsonObject.create().put("address", FieldAddress)))
            .put("age", 29)
          val encoded: Array[Byte] = DefaultEncoder.INSTANCE.encode(json).content()
        }
      }
    }

    performance of "JsonObject (Scala)" in {
      import com.couchbase.client.scala.json._
      using(gen) in {
        r => {
          val json = JsonObject.create
            .put("name", FieldName)
            .put("address", JsonArray(JsonObject.create.put("address", FieldAddress)))
            .put("age", 29)
          val encoded: Array[Byte] = Conversions.encode(json).get._1
        }
      }
    }

    performance of "JsoniterObject (Scala)" in {
      using(gen) in {
        r => {
          val json = experiments.JsoniterObject.create
            .put("name", FieldName)
            .put("address", experiments.JsoniterArray(ArrayBuffer(experiments.JsoniterObject.create.put("address", FieldName))))
            .put("age", 29)
          val encoded: Array[Byte] = Conversions.encode(json)(jsonobject.Encoders.JsoniterObjectConvert).get._1
        }
      }
    }

    performance of "Play" in {
      import play.api.libs.json.Json.{obj, arr}

      using(gen) in {
        r => {
          val json = obj("name" -> FieldName,
            "address" -> arr(obj("address" -> FieldAddress)),
            "age" -> 29)
          val encoded: Array[Byte] = Conversions.encode(json).get._1
        }
      }
    }

    performance of "Jawn" in {
      import org.typelevel.jawn.ast._

      using(gen) in {
        r => {
          val json = JObject.fromSeq(Seq("name" -> JString(FieldName),
            "address" -> JArray(Array(JObject.fromSeq(Seq("address" -> JString(FieldAddress))))),
            "age" -> JNum(29)))
          val encoded: Array[Byte] = Conversions.encode(json).get._1
        }
      }
    }
  }

}
