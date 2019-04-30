package com.couchbase.client.scala.encodings

import com.couchbase.client.core.error.DecodingFailedException
import com.couchbase.client.scala.codec.Conversions.Encodable
import com.couchbase.client.scala.codec.DocumentFlags
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection, TestUtils}
import com.couchbase.client.test.ClusterAwareIntegrationTest
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
class EncodingsSpec extends ScalaIntegrationTest {

  private var env: ClusterEnvironment = _
  private var cluster: Cluster = _
  private var coll: Collection = _

  @BeforeAll
  def beforeAll(): Unit = {
    val config = ClusterAwareIntegrationTest.config()
    env = environment.build
    cluster = Cluster.connect(env)
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.shutdown()
    env.shutdown()
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
  def encode_encoded_json_string() {
    val content = """{"hello":"world"}"""
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.Json)
  }

  @Test
  def encode_encoded_json_string_directly_as_string() {
    val content = """{"hello":"world"}"""
    val docId = TestUtils.docId()
    coll.insert(docId, content)(Encodable.AsValue.StringConvert).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.String)
  }

  @Test
  def decode_encoded_json_string_as_json() {
    val content = """{"hello":"world"}"""
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) =>
        assert(out("hello").str == "world")
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_encoded_json_string_as_string() {
    val content = """{"hello":"world"}"""
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[String] match {
      case Success(out) =>
        assert(out == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  @Test
  def encode_raw_string() {
    val content = """hello, world!"""
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    val doc = coll.get(docId).get

    // This is a bad assumption, the app should have specified string type
    assert(doc.flags == DocumentFlags.Json)
  }


  @Test
  def encode_raw_string_directly_as_string() {
    val content = """hello, world!"""
    val docId = TestUtils.docId()
    coll.insert(docId, content)(Encodable.AsValue.StringConvert).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.String)
  }


  @Test
  def decode_raw_string_as_json_should_fail() {
    val content = """hello, world!"""
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) => assert(false, "should not succeed")
      case Failure(err: DecodingFailedException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_raw_string_written_directly_as_json_into_json() {
    val content = """hello, world!"""
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) => assert(false, "should not succeed")
      case Failure(err: DecodingFailedException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_raw_string_as_string() {
    val content = """hello, world!"""
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[String] match {
      case Success(out) =>
        assert(out == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_raw_string_written_directly_as_string_into() {
    val content = """hello, world!"""
    val docId = TestUtils.docId()
    coll.insert(docId, content)(Encodable.AsValue.StringConvert).get

    coll.get(docId).get.contentAs[String] match {
      case Success(out) =>
        assert(out == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  @Test
  def encode_json_bytes() {
    val content = ujson.Obj("hello" -> "world")
    val encoded: Array[Byte] = ujson.transform(content, ujson.BytesRenderer()).toBytes
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.Json)
  }

  @Test
  def decode_json_bytes_as_json() {
    val content = ujson.Obj("hello" -> "world")
    val encoded: Array[Byte] = ujson.transform(content, ujson.BytesRenderer()).toBytes
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) =>
        assert(out("hello").str == "world")
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_json_bytes_as_string() {
    val content = ujson.Obj("hello" -> "world")
    val encoded: Array[Byte] = ujson.transform(content, ujson.BytesRenderer()).toBytes
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[String] match {
      case Success(out) =>
        assert(out == """{"hello":"world"}""")
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_json_bytes_written_directly_as_json_into() {
    val content = ujson.Obj("hello" -> "world")
    val encoded: Array[Byte] = ujson.transform(content, ujson.BytesRenderer()).toBytes
    val docId = TestUtils.docId()
    coll.insert(docId, encoded).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) =>
        assert(out("hello").str == "world")
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_json_bytes_written_directly_as_binary_into() {
    val content = ujson.Obj("hello" -> "world")
    val encoded: Array[Byte] = ujson.transform(content, ujson.BytesRenderer()).toBytes
    val docId = TestUtils.docId()
    coll.insert(docId, encoded)(Encodable.AsValue.BytesConvert).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) =>
      // stored as binary but it's still legit json, seems ok to be able to decode as json
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def encode_raw_bytes() {
    val content = Array[Byte](1, 2, 3, 4)
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    val doc = coll.get(docId).get

    // This is a bad assumption, the app should have specified binary type
    assert(doc.flags == DocumentFlags.Json)
  }

  @Test
  def raw_json_bytes_as_json_should_fail() {
    val content = Array[Byte](1, 2, 3, 4)
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) => assert(false, "should not succeed")
      case Failure(err: DecodingFailedException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_raw_bytes_as_string_should_fail() {
    val content = Array[Byte](1, 2, 3, 4)
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) => assert(false, "should not succeed")
      case Failure(err: DecodingFailedException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_raw_bytes_written_directly_as_binary_as_string() {
    val content = Array[Byte](1, 2, 3, 4)
    val docId = TestUtils.docId()
    coll.insert(docId, content)(Encodable.AsValue.BytesConvert).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) => assert(false, "should not succeed")
      case Failure(err: DecodingFailedException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def decode_raw_bytes_as_bytes() {
    val content = Array[Byte](1, 2, 3, 4)
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    assert(coll.get(docId).get.contentAsBytes sameElements content)
  }

  @Test
  def decode_raw_bytes_written_directly_as_binary_as() {
    val content = Array[Byte](1, 2, 3, 4)
    val docId = TestUtils.docId()
    coll.insert(docId, content)(Encodable.AsValue.BytesConvert).get

    assert(coll.get(docId).get.contentAsBytes sameElements content)
  }
}
