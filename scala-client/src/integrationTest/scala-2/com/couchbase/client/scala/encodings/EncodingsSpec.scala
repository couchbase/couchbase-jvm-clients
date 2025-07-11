/*
 * Copyright (c) 2025 Couchbase, Inc.
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
package com.couchbase.client.scala.encodings

import java.util.concurrent.TimeUnit
import com.couchbase.client.core.error.DecodingFailureException
import com.couchbase.client.scala.codec.JsonSerializer
import com.couchbase.client.scala.codec.{
  DocumentFlags,
  RawBinaryTranscoder,
  RawJsonTranscoder,
  RawStringTranscoder
}
import com.couchbase.client.scala.kv.{GetOptions, InsertOptions}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection, TestUtils}
import com.couchbase.client.test.IgnoreWhen
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
class EncodingsSpec extends ScalaIntegrationTest {

  private var cluster: Cluster = _
  private var coll: Collection = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
    bucket.waitUntilReady(WaitUntilReadyDefault)
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  def getContent(docId: String): ujson.Obj = {
    coll.get(docId) match {
      case Success(result) =>
        result.contentAs[ujson.Obj] match {
          case Success(content) =>
            content
          case Failure(err) =>
            assert(false, s"unexpected error $err")
            null
        }
      case Failure(err) =>
        assert(false, s"unexpected error $err")
        null
    }
  }

  @Test
  def encode_encoded_json_string(): Unit = {
    val content = """{"hello":"world"}"""
    val docId   = TestUtils.docId()
    coll.insert(docId, content).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.Json)
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true) // Needs ING-374
  @Test
  def encode_encoded_json_string_directly_as_string(): Unit = {
    val content = """{"hello":"world"}"""
    val docId   = TestUtils.docId()
    coll.insert(docId, content, InsertOptions().transcoder(RawStringTranscoder.Instance)).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.String)
  }

  @Test
  def decode_encoded_json_string_as_json(): Unit = {
    val content = """{"hello":"world"}"""
    val docId   = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out)                           => assert(false)
      case Failure(err: DecodingFailureException) => // ujson.Str cannot be cast to ujson.Obj
      case Failure(err)                           => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_encoded_json_string(): Unit = {
    val content = """{"hello":"world"}"""
    val docId   = TestUtils.docId()
    coll.insert(docId, content, InsertOptions().transcoder(RawJsonTranscoder.Instance)).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) =>
        assert(out("hello").str == "world")
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.get(docId).get.contentAs[String] match {
      case Success(out)                           => assert(false)
      case Failure(err: DecodingFailureException) =>
      case Failure(err)                           => assert(false, s"unexpected error $err")
    }

    coll
      .get(docId, GetOptions().transcoder(RawJsonTranscoder.Instance))
      .get
      .contentAs[String] match {
      case Success(out) => assert(out == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll
      .get(docId, GetOptions().transcoder(RawStringTranscoder.Instance))
      .get
      .contentAs[String] match {
      case Success(out) => assert(out == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }
  @Test
  def encode_raw_string(): Unit = {
    val content = """hello, world!"""
    val docId   = TestUtils.docId()
    coll.insert(docId, content).get

    val doc = coll.get(docId).get

    // The content is legal Json, but app probably meant to use RawStringTranscoder
    assert(doc.flags == DocumentFlags.Json)
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true) // Needs ING-374
  @Test
  def encode_raw_string_directly_as_string(): Unit = {
    val content = """hello, world!"""
    val docId   = TestUtils.docId()
    coll.insert(docId, content, InsertOptions().transcoder(RawStringTranscoder.Instance)).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.String)
  }
  @Test
  def decode_raw_string_as_json_should_fail(): Unit = {
    val content = """hello, world!"""
    val docId   = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out)                           => assert(false, "should not succeed")
      case Failure(err: DecodingFailureException) =>
      case Failure(err)                           => assert(false, s"unexpected error $err")
    }
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true) // Needs ING-374
  @Test
  def decode_raw_string(): Unit = {
    val content = """hello, world!"""
    val docId   = TestUtils.docId()
    coll.insert(docId, content, InsertOptions().transcoder(RawStringTranscoder.Instance)).get

    coll.get(docId).get.contentAs[String] match {
      case Success(out)                           => assert(false)
      case Failure(err: DecodingFailureException) =>
      case Failure(err)                           => assert(false, s"unexpected error $err")
    }

    coll
      .get(docId, GetOptions().transcoder(RawStringTranscoder.Instance))
      .get
      .contentAs[String] match {
      case Success(out) => assert(out == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }
  @Test
  def encode_json_bytes(): Unit = {
    val content              = ujson.Obj("hello" -> "world")
    val encoded: Array[Byte] = ujson.transform(content, ujson.BytesRenderer()).toByteArray
    val docId                = TestUtils.docId()
    coll.insert(docId, encoded, InsertOptions().transcoder(RawJsonTranscoder.Instance)).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.Json)
  }

  @Test
  def decode_json_bytes_as_json(): Unit = {
    val content              = ujson.Obj("hello" -> "world")
    val encoded: Array[Byte] = ujson.transform(content, ujson.BytesRenderer()).toByteArray
    val docId                = TestUtils.docId()
    coll.insert(docId, encoded, InsertOptions().transcoder(RawJsonTranscoder.Instance)).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) =>
        assert(out("hello").str == "world")
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_json_bytes_as_string(): Unit = {
    val content              = ujson.Obj("hello" -> "world")
    val encoded: Array[Byte] = ujson.transform(content, ujson.BytesRenderer()).toByteArray
    val docId                = TestUtils.docId()
    coll.insert(docId, encoded, InsertOptions().transcoder(RawJsonTranscoder.Instance)).get

    coll.get(docId).get.contentAs[String] match {
      case Success(out)                           => assert(false)
      case Failure(err: DecodingFailureException) =>
      case Failure(err)                           => assert(false, s"unexpected error $err")
    }

    coll
      .get(docId, GetOptions().transcoder(RawJsonTranscoder.Instance))
      .get
      .contentAs[String] match {
      case Success(out) => assert(out == content.toString)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_json_bytes_as_string_with_transcoder(): Unit = {
    val content              = ujson.Obj("hello" -> "world")
    val encoded: Array[Byte] = ujson.transform(content, ujson.BytesRenderer()).toByteArray
    val docId                = TestUtils.docId()
    coll.insert(docId, encoded, InsertOptions().transcoder(RawJsonTranscoder.Instance)).get

    coll
      .get(docId, GetOptions().transcoder(RawStringTranscoder.Instance))
      .get
      .contentAs[String] match {
      case Success(out) => assert(out == """{"hello":"world"}""")
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_json_bytes_written_directly_as_json_into(): Unit = {
    val content              = ujson.Obj("hello" -> "world")
    val encoded: Array[Byte] = ujson.transform(content, ujson.BytesRenderer()).toByteArray
    val docId                = TestUtils.docId()
    coll.insert(docId, encoded, InsertOptions().transcoder(RawJsonTranscoder.Instance)).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) =>
        assert(out("hello").str == "world")
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_json_bytes_written_directly_as_binary_into(): Unit = {
    val content              = ujson.Obj("hello" -> "world")
    val encoded: Array[Byte] = ujson.transform(content, ujson.BytesRenderer()).toByteArray
    val docId                = TestUtils.docId()
    coll.insert(docId, encoded, InsertOptions().transcoder(RawBinaryTranscoder.Instance)).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) =>
      // stored as binary but it's still legit json, seems ok to be able to decode as json
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  // This test has found a legitimate bug in that the insert is not creating a binary document in Protostellar mode, but
  // under ING-374 how flags are handled in Protostellar is being redone anyway.
  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  def encode_raw_bytes(): Unit = {
    val content = Array[Byte](1, 2, 3, 4)
    val docId   = TestUtils.docId()
    coll.insert(docId, content, InsertOptions().transcoder(RawBinaryTranscoder.Instance)).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.Binary)
  }

  @Test
  def raw_json_bytes_as_json_should_fail(): Unit = {
    val content = Array[Byte](1, 2, 3, 4)
    val docId   = TestUtils.docId()
    coll.insert(docId, content, InsertOptions().transcoder(RawJsonTranscoder.Instance)).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out)                           => assert(false, "should not succeed")
      case Failure(err: DecodingFailureException) =>
      case Failure(err)                           => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_raw_bytes_written_directly_as_binary_as_string(): Unit = {
    val content = Array[Byte](1, 2, 3, 4)
    val docId   = TestUtils.docId()
    coll.insert(docId, content, InsertOptions().transcoder(RawBinaryTranscoder.Instance)).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out)                           => assert(false, "should not succeed")
      case Failure(err: DecodingFailureException) =>
      case Failure(err)                           => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_raw_bytes_as_bytes(): Unit = {
    val content = Array[Byte](1, 2, 3, 4)
    val docId   = TestUtils.docId()
    coll.insert(docId, content, InsertOptions().transcoder(RawJsonTranscoder.Instance)).get

    assert(
      coll
        .get(docId, GetOptions().transcoder(RawJsonTranscoder.Instance))
        .get
        .contentAs[Array[Byte]]
        .get sameElements content
    )
  }

  @Test
  def decode_raw_bytes_written_directly_as_binary_as(): Unit = {
    val content = Array[Byte](1, 2, 3, 4)
    val docId   = TestUtils.docId()
    coll.insert(docId, content, InsertOptions().transcoder(RawBinaryTranscoder.Instance)).get

    assert(
      coll
        .get(docId, GetOptions().transcoder(RawBinaryTranscoder.Instance))
        .get
        .contentAs[Array[Byte]]
        .get sameElements content
    )
  }
}
