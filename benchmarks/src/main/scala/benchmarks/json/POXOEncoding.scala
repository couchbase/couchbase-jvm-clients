///*
// * Copyright (c) 2005, 2013, Oracle and/or its affiliates. All rights reserved.
// * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
// *
// * This code is free software; you can redistribute it and/or modify it
// * under the terms of the GNU General Public License version 2 only, as
// * published by the Free Software Foundation.  Oracle designates this
// * particular file as subject to the "Classpath" exception as provided
// * by Oracle in the LICENSE file that accompanied this code.
// *
// * This code is distributed in the hope that it will be useful, but WITHOUT
// * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
// * version 2 for more details (a copy is included in the LICENSE file that
// * accompanied this code).
// *
// * You should have received a copy of the GNU General Public License version
// * 2 along with this work; if not, write to the Free Software Foundation,
// * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
// *
// * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
// * or visit www.oracle.com if you need additional information or have any
// * questions.
// */
//package benchmarks
//
//import com.couchbase.client.java.codec.DefaultEncoder
//import com.couchbase.client.java.json.JsonObject
//import com.couchbase.client.scala.document.Conversions
//import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
//import com.github.plokhotnyuk.jsoniter_scala.macros.{CodecMakerConfig, JsonCodecMaker}
//import org.openjdk.jmh.annotations.Benchmark
//import org.openjdk.jmh.runner.Runner
//import org.openjdk.jmh.runner.RunnerException
//import org.openjdk.jmh.runner.options.Options
//import org.openjdk.jmh.runner.options.OptionsBuilder
//import ujson.BytesRenderer
//import com.github.plokhotnyuk.jsoniter_scala.macros._
//import com.github.plokhotnyuk.jsoniter_scala.core._
//import io.circe.{Decoder, Encoder, generic}
//import io.circe._, io.circe.generic.semiauto._
//
//case class Address(address: String)
//case class User(name: String, age: Int, addresses: List[Address])
//
////object Address {
////  implicit val rw: upickle.default.ReadWriter[Address] = upickle.default.macroRW
////  implicit val codec: JsonValueCodec[Address] = JsonCodecMaker.make[Address](CodecMakerConfig())
////  implicit val decoder: Decoder[Address] = deriveDecoder
////
////  implicit val encoder: Encoder[Address] = deriveEncoder
////}
////
////object User {
////  implicit val rw: upickle.default.ReadWriter[User] = upickle.default.macroRW
////  implicit val codec: JsonValueCodec[User] = JsonCodecMaker.make[User](CodecMakerConfig())
////  implicit val decoder: Decoder[User] = deriveDecoder
////  implicit val encoder: Encoder[User] = deriveEncoder
////}
//
////class POXOEncodingScala {
////  @Benchmark
////  def upickl(): Unit = {
////    val user = User("John Smith", 22, List(Address("123 Main Street")))
////    // TODO surely can go direct to bytes?
////    val json: ujson.Value = upickle.default.writeJs(user)
////    val encoded: Array[Byte] = Conversions.encode(json).get._1
////  }
////
////  @Benchmark
////  def circeScala(): Unit = {
////    import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
////    val user = User("John Smith", 22, List(Address("123 Main Street")))
////
////    val json: io.circe.Json = user.asJson
////    val encoded: Array[Byte] = Conversions.encode(json).get._1
////  }
////
////  @Benchmark
////  def jsoniterScala(): Unit = {
////    val user = User("John Smith", 22, List(Address("123 Main Street")))
////    val json: Array[Byte] = writeToArray(user)
////    val encoded: Array[Byte] = Conversions.encode(json).get._1
////  }
////}