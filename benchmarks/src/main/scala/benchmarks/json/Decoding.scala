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
//package json
//
//import com.couchbase.client.java.codec.{Decoder, DefaultDecoder, DefaultEncoder}
//import com.couchbase.client.java.json.JsonObject
//import com.couchbase.client.java.kv.EncodedDocument
//import com.couchbase.client.scala.document.{Conversions, DocumentFlags}
//import org.openjdk.jmh.annotations.Benchmark
//import play.api.libs.json.{JsNumber, JsObject, JsString}
//
//object Decoding {
//  val json = ujson.Obj("hello" -> "world", "foo" -> "bar", "age" -> 22)
//  val encoded: Array[Byte] = Conversions.encode(json).get._1
//  val encoded2 = new EncodedDocument(0, encoded)
//}
//
//class Decoding {
//
//  @Benchmark def upickleScala(): Unit = {
//    val decoded: ujson.Obj = Conversions.decode[ujson.Obj](Decoding.encoded).get
//  }
//
//
//  @Benchmark def playScala(): Unit = {
//    val json = JsObject(Seq("hello" -> JsString("world"), "foo" -> JsString("bar"), "age" -> JsNumber(22)))
//    val encoded: Array[Byte] = Conversions.encode(json).get._1
//  }
//
//  def contentAs[T](target: Class[T]): T = contentAs(target, DefaultDecoder.INSTANCE.asInstanceOf[Decoder[T]])
//
//  def contentAs[T](target: Class[T], decoder: Decoder[T]): T = decoder.decode(target, Decoding.encoded2)
//
//  @Benchmark def JsonObjectJava(): Unit = {
//    val decoded = contentAs(classOf[JsonObject])
//  }
//
//}
//
//
///*
//
//public class Decoding2 {
//    static String json = "{\"hello\":\"world\", \"foo\":\"bar\", \"age\": 22}";
//    static byte[] encoded = DefaultEncoder.INSTANCE.encode(json).content();
//    static EncodedDocument doc = new EncodedDocument(0, encoded);
//
//
//    public <T> T contentAs(final Class<T> target) {
//        return contentAs(target, (Decoder<T>) DefaultDecoder.INSTANCE);
//    }
//
//    public <T> T contentAs(final Class<T> target, final Decoder<T> decoder) {
//        return decoder.decode(target, doc);
//    }
//
//    @Benchmark
//    public void JsonObjectJava() {
//        JsonObject decoded = contentAs(JsonObject.class);
//    }
//
//}
// */