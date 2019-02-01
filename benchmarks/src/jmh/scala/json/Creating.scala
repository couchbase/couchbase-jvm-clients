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
package benchmarks

import com.couchbase.client.java.codec.DefaultEncoder
import com.couchbase.client.java.json.JsonObject
import com.couchbase.client.scala.document.Conversions
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.runner.Runner
import org.openjdk.jmh.runner.RunnerException
import org.openjdk.jmh.runner.options.Options
import org.openjdk.jmh.runner.options.OptionsBuilder


class Creating {
  @Benchmark def upickleScala(): Unit = {
    val json = ujson.Obj("hello" -> "world", "foo" -> "bar", "age" -> 22)
  }

//  @Benchmark def upickle2Scala(): Unit = {
//    val json = ujson.Obj.from(Seq("hello" -> "world", "foo" -> "bar", "age" -> 22))
//  }

  //  @Benchmark def circeScala(): Unit = {
//    import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
//
//    val json: io.circe.Json = Map("hello" -> "world",
//      "foo" -> "bar",
//    "age" -> 22).asJson
//  }

  @Benchmark def JsonObjectJava(): Unit = {
    val json = JsonObject.create()
      .put("hello", "world")
      .put("foo", "bar")
      .put("age", 22)
  }
}