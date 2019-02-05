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

import com.couchbase.client.java.codec.{Decoder, DefaultDecoder, DefaultEncoder}
import com.couchbase.client.java.json.JsonObject
import com.couchbase.client.java.kv.EncodedDocument
import com.couchbase.client.scala.document.Conversions
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.{CodecMakerConfig, JsonCodecMaker}
import io.circe.Encoder
import play.api.libs.json.{JsNumber, JsObject, JsString, JsValue}
//import org.openjdk.jmh.annotations.Benchmark
//import org.openjdk.jmh.runner.Runner
//import org.openjdk.jmh.runner.RunnerException
//import org.openjdk.jmh.runner.options.Options
//import org.openjdk.jmh.runner.options.OptionsBuilder

import org.scalameter.api._


object Creating extends Bench.ForkedTime {
  val gen = Gen.unit("num")

  performance of "Just creating JSON AST" in {
    performance of "upickle" in {
      using(gen) in {
        r => val json = ujson.Obj("hello" -> "world", "foo" -> "bar", "age" -> 22)
      }
    }

        performance of "upickle (alternative creation method)" in {
          using(gen) in {
            r => val json = ujson.Obj.from(Seq("hello" -> "world", "foo" -> "bar", "age" -> 22))
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

      case class Address(address: String)
      case class User(name: String, age: Int, addresses: List[Address])

      object Address {
        implicit val rw: upickle.default.ReadWriter[Address] = upickle.default.macroRW
        implicit val codec: JsonValueCodec[Address] = JsonCodecMaker.make[Address](CodecMakerConfig())
//        implicit val decoder: Decoder[Address] = deriveDecoder
//        implicit val encoder: Encoder[Address] = deriveEncoder
      }

      object User {
        implicit val rw: upickle.default.ReadWriter[User] = upickle.default.macroRW
        implicit val codec: JsonValueCodec[User] = JsonCodecMaker.make[User](CodecMakerConfig())
//        implicit val decoder: Decoder[User] = deriveDecoder
//        implicit val encoder: Encoder[User] = deriveEncoder
      }


      //    performance of "upickle" in {
  //      using(gen) in {
  //        r => {
  //        }
  //      }
  //    }

}

    performance of "Creating JSON AST and encoding to byte array" in {

//    performance of "upickle" in {
//      using(gen) in {
//        r => {
//        }
//      }
//    }

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

    // TODO
//    performance of "JsonObject" in {
//
//      val json = "{\"hello\":\"world\", \"foo\":\"bar\", \"age\": 22}";
//      val encoded = DefaultEncoder.INSTANCE.encode(json).content();
//      val doc = new EncodedDocument(0, encoded);
//
////        def contentAs[T](target: Class[T]): T = contentAs(target, DefaultDecoder.INSTANCE.asInstanceOf[Decoder[T]])
////
////        def contentAs[T](target: Class[T], decoder: Decoder[T]): T = decoder.decode(target, doc)
//
//
//      using(gen) in {
//        r => {
//          DefaultDecoder.INSTANCE.asInstanceOf[Decoder[JsonObject]].decode(classOf[JsonObject], doc)
////          val decoded = contentAs(classOf[JsonObject])
//        }
//      }
//    }

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
