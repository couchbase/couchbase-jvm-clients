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

import com.couchbase.client.java.codec.DefaultEncoder
import com.couchbase.client
import com.couchbase.client.java.json.JacksonTransformers
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.codec.Conversions.Encodable
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalameter.api._
import play.api.libs.json.JsObject
import ujson.BytesRenderer


// A deep dive into the performance of the Scala JsonObject
object JsonObjectBench extends Bench.LocalTime {
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
          val json = com.couchbase.client.scala.json.JsonObject.create
            .put("hello", "world")
            .put("foo", "bar")
            .put("age", 22)
      }
    }
  }




  performance of "Creating JSON AST and encoding to byte array" in {


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
  }
}
