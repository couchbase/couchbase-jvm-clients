package com.couchbase.client.scala.subdoc

import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.error.DecodingFailedException
import com.couchbase.client.core.error.subdoc.PathNotFoundException
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.json.JsonArray
import com.couchbase.client.scala.kv.{LookupInSpec, MutateInSpec}
import com.couchbase.client.scala.kv.LookupInSpec._
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection, TestUtils}
import com.couchbase.client.test.ClusterAwareIntegrationTest
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
class SubdocGetSpec extends ScalaIntegrationTest {

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

  @Test
  def no_commands() {
    val docId = TestUtils.docId()
    coll.lookupIn(docId, Array[LookupInSpec]()) match {
      case Success(result) => assert(false, s"unexpected success")
      case Failure(err: IllegalArgumentException) =>
      case Failure(err) =>
        assert(false, s"unexpected error $err")
    }
  }


  @Test
  def lookupIn() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world",
      "foo" -> "bar",
      "age" -> 22)
    val insertResult = coll.insert(docId, content).get

    coll.lookupIn(docId, Array(get("foo"), get("age"))) match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas == insertResult.cas)
        assert(result.contentAs[String](0).get == "bar")
        assert(result.contentAs[Int](1).get == 22)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  @Test
  def get_array() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("animals" -> ujson.Arr("cat", "dog"))
    val insertResult = coll.insert(docId, content).get

    coll.lookupIn(docId, Array(get("animals"))) match {
      case Success(result) =>
        assert(result.contentAsBytes(0).get sameElements """["cat","dog"]""".getBytes(CharsetUtil.UTF_8))
        assert(result.contentAs[String](0).get == """["cat","dog"]""")
        assert(result.contentAs[ujson.Arr](0).get == ujson.Arr("cat", "dog"))
        assert(result.contentAs[JsonArray](0).get == JsonArray("cat", "dog"))
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def path_does_not_exist_single() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content).get

    coll.lookupIn(docId, Array(get("not_exist"))) match {
      case Success(result) => assert(false, s"should not succeed")
      case Failure(err: PathNotFoundException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def path_does_not_exist_multi() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content).get

    coll.lookupIn(docId, Array(get("not_exist"), get("hello"))) match {
      case Success(result) =>
        assert(result.cas != 0)
        result.contentAs[String](0) match {
          case Success(body) => assert(false, s"should not succeed")
          case Failure(err: PathNotFoundException) =>
          case Failure(err) => assert(false, s"unexpected error $err")
        }
        assert(result.contentAs[String](1).get == "world")
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def lookupIn_with_doc() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world",
      "foo" -> "bar",
      "age" -> 22)
    val insertResult = coll.insert(docId, content).get

    coll.lookupIn(docId, Array(get("foo"), get("age"), getDoc)) match {
      case Success(result) =>
        assert(result.contentAs[String](0).get == "bar")
        result.contentAs[ujson.Obj](2) match {
          case Success(body) =>
            assert(body("hello").str == "world")
            assert(body("age").num == 22)
          case Failure(err) => assert(false, s"unexpected error $err")
        }
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def exists_single() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> ujson.Arr("world"))
    val insertResult = coll.insert(docId, content).get

    coll.lookupIn(docId,
      Array(exists("does_not_exist"))) match {
      case Success(result) =>
        result.contentAs[Boolean](0) match {
          case Failure(err: PathNotFoundException) =>
          case Success(v) => assert(false, s"should not succeed")
          case Failure(err) => assert(false, s"unexpected error $err")
        }
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def exists_multi() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> ujson.Arr("world"),
      "foo" -> "bar",
      "age" -> 22)
    val insertResult = coll.insert(docId, content).get

    coll.lookupIn(docId,
      Array(LookupInSpec.count("hello"), LookupInSpec.exists("age"), LookupInSpec.exists("does_not_exist"))) match {
      case Success(result) =>
        assert(result.exists(0))
        assert(result.exists(1))
        assert(!result.exists(2))
        assert(result.contentAs[Boolean](1).get)
        result.contentAs[Boolean](2) match {
          case Failure(err: PathNotFoundException) =>
          case Success(v) => assert(false, s"should not succeed")
          case Failure(err) => assert(false, s"unexpected error $err")
        }
        result.contentAs[String](1) match {
          case Failure(err: DecodingFailedException) =>
          case Success(v) => assert(false, s"should not succeed")
          case Failure(err) => assert(false, s"unexpected error $err")
        }
        assert(result.contentAs[Int](0).get == 1)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def count() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> ujson.Arr("world"),
      "foo" -> "bar",
      "age" -> 22)
    val insertResult = coll.insert(docId, content).get

    coll.lookupIn(docId,
      Array(LookupInSpec.count("hello"), exists("age"), exists("does_not_exist"))) match {
      case Success(result) =>
        assert(result.contentAs[Boolean](1).get)
        result.contentAs[Boolean](2) match {
          case Failure(err: PathNotFoundException) =>
          case Success(v) => assert(false, s"should not succeed")
          case Failure(err) => assert(false, s"unexpected error $err")
        }
        result.contentAs[String](1) match {
          case Failure(err: DecodingFailedException) =>
          case Success(v) => assert(false, s"should not succeed")
          case Failure(err) => assert(false, s"unexpected error $err")
        }
        assert(result.contentAs[Int](0).get == 1)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }
}
