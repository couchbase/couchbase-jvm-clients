package com.couchbase.client.scala.encodings

import com.couchbase.client.core.error.DecodingFailedException
import com.couchbase.client.core.error.subdoc.MultiMutationException
import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus
import com.couchbase.client.scala.api.MutateInSpec
import com.couchbase.client.scala.codec.Conversions.Encodable
import com.couchbase.client.scala.document.DocumentFlags
import com.couchbase.client.scala.{Cluster, TestUtils}
import org.scalatest.FunSuite

import scala.util.{Failure, Success}

class EncodingsSpec extends FunSuite {
  // TODO support Jenkins
  val (cluster, bucket, coll) = (for {
    cluster <- Cluster.connect("localhost", "Administrator", "password")
    bucket <- cluster.bucket("default")
    coll <- bucket.defaultCollection()
  } yield (cluster, bucket, coll)) match {
    case Success(result) => result
    case Failure(err) => throw err
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

  test("encode encoded-json-string") {
    val content = """{"hello":"world"}"""
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.Json)
  }

  test("encode encoded-json-string directly as json") {
    val content = """{"hello":"world"}"""
    val docId = TestUtils.docId()
    coll.insert(docId, content)(Encodable.AsJson.StringConvert).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.Json)
  }

  test("encode encoded-json-string directly as string") {
    val content = """{"hello":"world"}"""
    val docId = TestUtils.docId()
    coll.insert(docId, content)(Encodable.AsValue.StringConvert).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.String)
  }

  test("decode encoded-json-string as json") {
    val content = """{"hello":"world"}"""
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) =>
        assert(out("hello").str == "world")
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("decode encoded-json-string as string") {
    val content = """{"hello":"world"}"""
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[String] match {
      case Success(out) =>
        assert(out == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }



  test("encode raw-string") {
    val content = """hello, world!"""
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.String)
  }


  test("encode raw-string directly as string") {
    val content = """hello, world!"""
    val docId = TestUtils.docId()
    coll.insert(docId, content)(Encodable.AsValue.StringConvert).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.String)
  }


  test("encode raw-string directly as json") {
    val content = """hello, world!"""
    val docId = TestUtils.docId()
    coll.insert(docId, content)(Encodable.AsJson.StringConvert).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.Json)
  }

  test("decode raw-string as json should fail") {
    val content = """hello, world!"""
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) => assert(false, "should not succeed")
      case Failure(err: DecodingFailedException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("decode raw-string written directly as json, into json, should fail") {
    val content = """hello, world!"""
    val docId = TestUtils.docId()
    coll.insert(docId, content)(Encodable.AsJson.StringConvert).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) => assert(false, "should not succeed")
      case Failure(err: DecodingFailedException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("decode raw-string as string") {
    val content = """hello, world!"""
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[String] match {
      case Success(out) =>
        assert(out == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("decode raw-string written directly as string, into string") {
    val content = """hello, world!"""
    val docId = TestUtils.docId()
    coll.insert(docId, content)(Encodable.AsValue.StringConvert).get

    coll.get(docId).get.contentAs[String] match {
      case Success(out) =>
        assert(out == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }



  test("encode json-bytes") {
    val content = ujson.Obj("hello" -> "world")
    val encoded: Array[Byte] = ujson.transform(content, ujson.BytesRenderer()).toBytes
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.Json)
  }

  test("decode json-bytes as json") {
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

  test("decode json-bytes as string") {
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

  test("decode json-bytes, written directly as json, into json") {
    val content = ujson.Obj("hello" -> "world")
    val encoded: Array[Byte] = ujson.transform(content, ujson.BytesRenderer()).toBytes
    val docId = TestUtils.docId()
    coll.insert(docId, encoded)(Encodable.AsJson.BytesConvert).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) =>
        assert(out("hello").str == "world")
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("decode json-bytes, written directly as binary, into json") {
    val content = ujson.Obj("hello" -> "world")
    val encoded: Array[Byte] = ujson.transform(content, ujson.BytesRenderer()).toBytes
    val docId = TestUtils.docId()
    coll.insert(docId, encoded)(Encodable.AsJson.BytesConvert).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) =>
      case Success(out) => assert(false, "should not succeed")
      case Failure(err: DecodingFailedException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }



  test("encode raw-bytes") {
    val content = Array[Byte](1, 2, 3, 4)
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    val doc = coll.get(docId).get

    assert(doc.flags == DocumentFlags.Binary)
  }

  test("raw json-bytes as json should fail") {
    val content = Array[Byte](1, 2, 3, 4)
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) => assert(false, "should not succeed")
      case Failure(err: DecodingFailedException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("decode raw-bytes as string should fail") {
    val content = Array[Byte](1, 2, 3, 4)
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) => assert(false, "should not succeed")
      case Failure(err: DecodingFailedException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("decode raw-bytes, written directly as binary, as string should fail") {
    val content = Array[Byte](1, 2, 3, 4)
    val docId = TestUtils.docId()
    coll.insert(docId, content)(Encodable.AsValue.BytesConvert).get

    coll.get(docId).get.contentAs[ujson.Obj] match {
      case Success(out) => assert(false, "should not succeed")
      case Failure(err: DecodingFailedException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("decode raw-bytes as bytes") {
    val content = Array[Byte](1, 2, 3, 4)
    val docId = TestUtils.docId()
    coll.insert(docId, content).get

    assert(coll.get(docId).get.contentAsBytes sameElements content)
  }

  test("decode raw-bytes, written directly as binary, as bytes") {
    val content = Array[Byte](1, 2, 3, 4)
    val docId = TestUtils.docId()
    coll.insert(docId, content)(Encodable.AsValue.BytesConvert).get

    assert(coll.get(docId).get.contentAsBytes sameElements content)
  }
}
