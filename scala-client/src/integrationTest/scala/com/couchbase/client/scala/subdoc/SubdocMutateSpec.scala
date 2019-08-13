package com.couchbase.client.scala.subdoc

import com.couchbase.client.core.error.KeyExistsException
import com.couchbase.client.core.error.subdoc.MultiMutationException
import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.LookupInSpec._
import com.couchbase.client.scala.kv.MutateInSpec._
import com.couchbase.client.scala.kv.{DocumentCreation, MutateInMacro, MutateInSpec}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection, TestUtils}
import com.couchbase.client.test.{Capabilities, ClusterAwareIntegrationTest, ClusterType, IgnoreWhen}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
class SubdocMutateSpec extends ScalaIntegrationTest {

  private var env: ClusterEnvironment = _
  private var cluster: Cluster = _
  private var coll: Collection = _

  @BeforeAll
  def beforeAll(): Unit = {
    val config = ClusterAwareIntegrationTest.config()
    env = environment.build.get
    cluster = Cluster.connect(env).get
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

  def getContent2(docId: String): JsonObject = {
    coll.get(docId) match {
      case Success(result) =>
        result.contentAs[JsonObject] match {
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

  def prepare(content: ujson.Value): (String, Long) = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val insertResult = coll.insert(docId, content).get
    (docId, insertResult.cas)
  }


  def prepareXattr(content: ujson.Value): (String, Long) = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val insertResult = coll.mutateIn(docId, Array(
      insert("x", content).xattr
    ), document = DocumentCreation.Insert).get
    (docId, insertResult.cas)
  }

  @Test
  def no_commands() {
    val docId = TestUtils.docId()
    coll.mutateIn(docId, Array[MutateInSpec]()) match {
      case Success(result) => assert(false, s"unexpected success")
      case Failure(err: IllegalArgumentException) =>
      case Failure(err) =>
        assert(false, s"unexpected error $err")
    }
  }


  @Test
  def insert_string() {
    val content = ujson.Obj("hello" -> "world",
      "foo" -> "bar",
      "age" -> 22)
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(insert("foo2", "bar2"))) match {
      case Success(result) => assert(result.cas != cas)
      case Failure(err) =>
        assert(false, s"unexpected error $err")
    }

    assert(getContent(docId)("foo2").str == "bar2")
  }

  @Test
  def upsert_existing_doc() {
    val content = ujson.Obj("hello" -> "world",
      "foo" -> "bar",
      "age" -> 22)
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(insert("foo2", "bar2")), document = DocumentCreation.Upsert) match {
      case Success(result) => assert(result.cas != cas)
      case Failure(err) =>
        assert(false, s"unexpected error $err")
    }

    assert(getContent(docId)("foo2").str == "bar2")
  }

  @Test
  def insert_existing_doc() {
    val content = ujson.Obj("hello" -> "world")
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(insert("foo2", "bar2")), document = DocumentCreation.Insert) match {
      case Success(result) => assert(false)
      case Failure(err: KeyExistsException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def insert_not_existing_doc() {
    val docId = TestUtils.docId()

    coll.mutateIn(docId, Array(insert("foo2", "bar2")), document = DocumentCreation.Insert) match {
      case Success(result) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    assert(getContent(docId)("foo2").str == "bar2")
  }

  @Test
  def upsert_not_existing_doc() {
    val docId = TestUtils.docId()

    coll.mutateIn(docId, Array(insert("foo2", "bar2")), document = DocumentCreation.Upsert) match {
      case Success(result) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    assert(getContent(docId)("foo2").str == "bar2")
  }


  @Test
  def remove() {
    val content = ujson.Obj("hello" -> "world",
      "foo" -> "bar",
      "age" -> 22)
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(MutateInSpec.remove("foo"))) match {
      case Success(result) => assert(result.cas != cas)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    Assertions.assertThrows(classOf[NoSuchElementException], () => (getContent(docId)("foo")))
  }


  private def checkSingleOpSuccess(content: ujson.Obj, ops: Seq[MutateInSpec]) = {
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, ops) match {
      case Success(result) =>
        assert(result.cas != cas)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.get(docId).get.contentAs[ujson.Obj].get
  }

  private def checkSingleOpSuccessXattr(content: ujson.Obj, ops: Seq[MutateInSpec]) = {
    val (docId, cas) = prepareXattr(content)

    coll.mutateIn(docId, ops) match {
      case Success(result) =>
        assert(result.cas != cas)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.lookupIn(docId, Array(get("x").xattr)).get.contentAs[ujson.Obj](0).get
  }

  private def checkSingleOpFailure(content: ujson.Obj, ops: Seq[MutateInSpec], expected: SubDocumentOpResponseStatus)
  = {
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, ops) match {
      case Success(result) => assert(false, "should not succeed")
      case Failure(err: MultiMutationException) =>
        assert(err.firstFailureStatus() == expected)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  private def checkSingleOpFailureXattr(content: ujson.Obj, ops: Seq[MutateInSpec],
                                        expected: SubDocumentOpResponseStatus) = {
    val (docId, cas) = prepareXattr(content)

    coll.mutateIn(docId, ops) match {
      case Success(result) => assert(false, "should not succeed")
      case Failure(err: MultiMutationException) =>
        assert(err.firstFailureStatus() == expected)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def insert_string_already_there() {
    checkSingleOpFailure(ujson.Obj("foo" -> "bar"), Array(insert("foo", "bar2")), SubDocumentOpResponseStatus
      .PATH_EXISTS)
  }

  @Test
  def insert_bool() {
    val content = ujson.Obj()
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(insert("foo2", false))) match {
      case Success(result) => assert(result.cas != cas)
      case Failure(err) =>
        assert(false, s"unexpected error $err")
    }

    assert(!getContent(docId)("foo2").bool)
  }

  @Test
  def replace_bool() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("hello" -> "world"),
      Array(replace("hello", false)))
    assert(!updatedContent("hello").bool)
  }

  @Test
  def insert_int() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(),
      Array(insert("hello", false)))
    assert(!updatedContent("hello").bool)
  }

  @Test
  def replace_int() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("hello" -> "world"),
      Array(replace("hello", 42)))
    assert(updatedContent("hello").num == 42)
  }

  @Test
  def replace_long() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("hello" -> "world"),
      Array(replace("hello", Long.MaxValue)))
    assert(updatedContent("hello").num == Long.MaxValue)
  }

  @Test
  def replace_double() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("hello" -> "world"),
      Array(replace("hello", 42.3)))
    assert(updatedContent("hello").num == 42.3)
  }

  @Test
  def replace_short() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("hello" -> "world"),
      Array(replace("hello", Short.MaxValue)))
    assert(updatedContent("hello").num == Short.MaxValue)
  }

  @Test
  def replace_string() {
    val content = ujson.Obj("hello" -> "world",
      "foo" -> "bar",
      "age" -> 22)
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(replace("foo", "bar2"))) match {
      case Success(result) => assert(result.cas != cas)
      case Failure(err) =>
        assert(false, s"unexpected error $err")
    }

    assert(getContent(docId)("foo").str == "bar2")
  }

  @Test
  def replace_string_does_not_exist() {
    checkSingleOpFailure(ujson.Obj(), Array(replace("foo", "bar2")), SubDocumentOpResponseStatus.PATH_NOT_FOUND)
  }

  @Test
  def upsert_string() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> "bar"), Array(upsert("foo", "bar2")))
    assert(updatedContent("foo").str == "bar2")
  }

  @Test
  def upsert_string_does_not_exist() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(), Array(upsert("foo", "bar2")))
    assert(updatedContent("foo").str == "bar2")
  }

  @Test
  def array_append() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Arr("hello")),
      Array(arrayAppend("foo", "world")))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "world"))
  }

  @Test
  def array_append_multi() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Arr("hello")),
      Array(arrayAppend("foo", "cruel", "world")))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "cruel", "world"))
  }

  @Test
  def array_prepend() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Arr("hello")),
      Array(arrayPrepend("foo", "world")))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("world", "hello"))
  }

  @Test
  def array_prepend_multi() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Arr("hello")),
      Array(arrayPrepend("foo", "cruel", "world")))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("cruel", "world", "hello"))
  }

  @Test
  def array_insert() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Arr("hello", "world")),
      Array(arrayInsert("foo[1]", "cruel")))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "cruel", "world"))
  }

  @Test
  def array_insert_multi() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Arr("hello", "world")),
      Array(arrayInsert("foo[1]", "cruel", "world2")))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "cruel", "world2", "world"))
  }

  @Test
  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  def array_insert_unique_does_not_exist() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Arr("hello", "world")),
      Array(arrayAddUnique("foo", "cruel")))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "world", "cruel"))
  }

  @Test
  def array_insert_unique_does_exist() {
    val updatedContent = checkSingleOpFailure(ujson.Obj("foo" -> ujson.Arr("hello", "cruel", "world")),
      Array(arrayAddUnique("foo", "cruel")),
      SubDocumentOpResponseStatus.PATH_EXISTS)
  }

  @Test
  def counter__5 () {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> 10),
      Array(increment("foo", 5)))
    assert(updatedContent("foo").num == 15)
  }

  @Test
  def counter_5_returned () {
    val content = ujson.Obj("foo" -> 10)
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(increment("foo", 5))) match {
      case Success(result) =>
        assert(result.contentAs[Long](0).get == 15)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def counter_minus5() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> 10),
      Array(decrement("foo", 3)))
    assert(updatedContent("foo").num == 7)
  }


  @Test
  def insert_xattr() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(), Array(insert("x.foo", "bar2").xattr))
    assert(updatedContent("foo").str == "bar2")
  }

  @Test
  def remove_xattr() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> "bar"), Array(MutateInSpec.remove("x.foo").xattr))
    Assertions.assertThrows(classOf[NoSuchElementException], () => (updatedContent("foo")))
  }

  @Test
  def remove_xattr_does_not_exist() {
    checkSingleOpFailureXattr(ujson.Obj(), Array(MutateInSpec.remove("x.foo").xattr), SubDocumentOpResponseStatus.PATH_NOT_FOUND)
  }

  @Test
  def insert_string_already_there_xattr() {
    checkSingleOpFailureXattr(ujson.Obj("foo" -> "bar"), Array(insert("x.foo", "bar2").xattr),
      SubDocumentOpResponseStatus.PATH_EXISTS)
  }

  @Test
  def replace_string_xattr() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> "bar"), Array(replace("x.foo", "bar2").xattr))
    assert(updatedContent("foo").str == "bar2")
  }

  @Test
  def replace_string_does_not_exist_xattr() {
    checkSingleOpFailure(ujson.Obj(), Array(replace("x.foo", "bar2").xattr), SubDocumentOpResponseStatus.PATH_NOT_FOUND)
  }

  @Test
  def upsert_string_xattr() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> "bar"), Array(upsert("x.foo", "bar2").xattr))
    assert(updatedContent("foo").str == "bar2")
  }

  @Test
  def upsert_string_does_not_exist_xattr() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(), Array(upsert("x.foo", "bar2").xattr))
    assert(updatedContent("foo").str == "bar2")
  }

  @Test
  def array_append_xattr() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> ujson.Arr("hello")),
      Array(arrayAppend("x.foo", "world").xattr))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "world"))
  }

  @Test
  def array_prepend_xattr() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> ujson.Arr("hello")),
      Array(arrayPrepend("x.foo", "world").xattr))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("world", "hello"))
  }

  @Test
  def array_insert_xattr() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> ujson.Arr("hello", "world")),
      Array(arrayInsert("x.foo[1]", "cruel").xattr))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "cruel", "world"))
  }

  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  @Test
  def array_insert_unique_does_not_exist_3() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> ujson.Arr("hello", "world")),
      Array(arrayAddUnique("x.foo", "cruel").xattr))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "world", "cruel"))
  }

  @Test
  def array_insert_unique_does_exist_xattr() {
    checkSingleOpFailureXattr(ujson.Obj("foo" -> ujson.Arr("hello", "cruel", "world")),
      Array(arrayAddUnique("x.foo", "cruel").xattr),
      SubDocumentOpResponseStatus.PATH_EXISTS)
  }

  @Test
  def counter_5_xatr() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> 10),
      Array(increment("x.foo", 5).xattr))
    assert(updatedContent("foo").num == 15)
  }

  @Test
  def counter_minus5_xatr() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> 10),
      Array(decrement("x.foo", 3).xattr))
    assert(updatedContent("foo").num == 7)
  }


  @Test
  def insert_expand_macro_xattr_do_not() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(), Array(insert("x.foo", "${Mutation.CAS}").xattr))
    assert(updatedContent("foo").str == "${Mutation.CAS}")
  }

  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  @Test
  def insert_expand_macro_xattr() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
      Array(insert("x.foo", MutateInMacro.MutationCAS).xattr))
    assert(updatedContent("foo").str != "${Mutation.CAS}")
  }


  @Test
  def insert_xattr_createPath() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(), Array(insert("x.foo.baz", "bar2").xattr.createPath))
    assert(updatedContent("foo").obj("baz").str == "bar2")
  }

  @Test
  def insert_string_already_there_xattr_createPath() {
    // Seems this should return PATH_EXISTS instead...
    checkSingleOpFailureXattr(ujson.Obj("foo" -> ujson.Obj("baz" -> "bar")), Array(insert("x.foo.baz", "bar2")),
      SubDocumentOpResponseStatus.PATH_NOT_FOUND)
  }

  @Test
  def upsert_string_xattr_createPath() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> ujson.Obj("baz" -> "bar")), Array(upsert("x" +
      ".foo", "bar2").xattr.createPath))
    assert(updatedContent("foo").str == "bar2")
  }

  @Test
  def upsert_string_does_not_exist_xattr_2() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(), Array(upsert("x.foo.baz", "bar2").xattr.createPath))
    assert(updatedContent("foo").obj("baz").str == "bar2")
  }

  @Test
  def array_append_xattr_createPath() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
      Array(arrayAppend("x.foo", "world").xattr.createPath))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("world"))
  }

  @Test
  def array_prepend_xattr_createPath() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
      Array(arrayPrepend("x.foo", "world").xattr.createPath))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("world"))
  }

  // Failing with bad input server error - investigate under SCBC-30
  @Disabled
  @Test
  def array_insert_xattr_createPath() {
        val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
          Array(arrayInsert("x.foo[0]", "cruel").xattr.createPath))
        assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("cruel"))
  }

  @Test
  def array_insert_unique_does_not_exist_xattr() {
        val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
          Array(arrayAddUnique("x.foo", "cruel").xattr.createPath))
        assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("cruel"))
  }


  @Test
  def counter_5_xattr_createPath() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
      Array(increment("x.foo", 5).xattr.createPath))
    assert(updatedContent("foo").num == 5)
  }

  @Test
  def counter_minus5_xattr_createPath() {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
      Array(decrement("x.foo", 3).xattr.createPath))
    assert(updatedContent("foo").num == -3)
  }


  @Test
  def insert_createPath() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(), Array(insert("foo.baz", "bar2").createPath))
    assert(updatedContent("foo").obj("baz").str == "bar2")
  }

  @Test
  def insert_string_already_there_createPath() {
    checkSingleOpFailure(ujson.Obj("foo" -> ujson.Obj("baz" -> "bar")), Array(insert("foo.baz", "bar2")),
      SubDocumentOpResponseStatus.PATH_EXISTS)
  }

  @Test
  def upsert_string_createPath() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Obj("baz" -> "bar")), Array(upsert("foo",
      "bar2").createPath))
    assert(updatedContent("foo").str == "bar2")
  }

  @Test
  def upsert_string_does_not_exist_createPath() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(), Array(upsert("foo.baz", "bar2").createPath))
    assert(updatedContent("foo").obj("baz").str == "bar2")
  }

  @Test
  def array_append_createPath() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(),
      Array(arrayAppend("foo", "world").createPath))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("world"))
  }

  @Test
  def array_prepend_createPath() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(),
      Array(arrayPrepend("foo", "world").createPath))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("world"))
  }

  // Will look at under SCBC-30
//  @Test
//  def array_insert_createPath() {
//        val updatedContent = checkSingleOpSuccess(ujson.Obj(),
//          Array(arrayInsert("foo[0]", "cruel").createPath))
//        assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("cruel"))
//  }

//  @Test
//  def array_insert_unique_does_not_exist_createPath() {
//        val updatedContent = checkSingleOpSuccess(ujson.Obj(),
//          Array(arrayAddUnique("foo", "cruel").createPath))
//        assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer(test"cruel"))
//  }

  @Test
  def counter_5_createPath() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(),
      Array(increment("foo", 5).createPath))
    assert(updatedContent("foo").num == 5)
  }

  @Test
  def counter_minus5_createPath() {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(),
      Array(decrement("foo", 3).createPath))
    assert(updatedContent("foo").num == -3)
  }

  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  @Test
  def expiration() {
    val content = ujson.Obj("hello" -> "world")
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(insert("foo2", "bar2")), expiration = 10.seconds) match {
      case Success(result) => assert(result.cas != cas)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.get(docId, withExpiration = true) match {
      case Success(result) =>
        assert(result.expiration.isDefined)
        assert(result.expiration.get.toSeconds != 0)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  @Test
  def moreThan16() {
    val content = ujson.Obj("hello" -> "world")
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(insert("foo0", "bar0")
      , insert("foo1", "bar1")
      , insert("foo2", "bar2")
      , insert("foo3", "bar3")
      , insert("foo4", "bar4")
      , insert("foo5", "bar5")
      , insert("foo6", "bar6")
      , insert("foo7", "bar7")
      , insert("foo8", "bar8")
      , insert("foo9", "bar9")
      , insert("foo10", "bar10")
      , insert("foo11", "bar11")
      , insert("foo12", "bar12")
      , insert("foo13", "bar13")
      , insert("foo14", "bar14")
      , insert("foo15", "bar15")
      , insert("foo16", "bar16"))) match {
      case Success(result) => assert(false, "should not succeed")
      case Failure(err: IllegalArgumentException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def two_commands_succeed() {
    val content = ujson.Obj("hello" -> "world")
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(insert("foo0", "bar0")
      , insert("foo1", "bar1")
      , insert("foo2", "bar2"))) match {
      case Success(result) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    val updated = getContent(docId)
    assert(updated("foo1").str == "bar1")
    assert(updated("foo2").str == "bar2")
  }


  @Test
  def two_commands_one_fails() {
    val content = ujson.Obj("foo1" -> "bar_orig_1", "foo2" -> "bar_orig_2")
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(insert("foo0", "bar0"),
      insert("foo1", "bar1"),
      MutateInSpec.remove("foo3"))) match {
      case Success(result) =>
      case Failure(err: MultiMutationException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    val updated = getContent(docId)
    assert(updated("foo1").str == "bar_orig_1")
  }

  @Test
  def write_and_read_primitive_boolean() {
    val docId = TestUtils.docId()
    assert(coll.mutateIn(docId, Array(upsert("foo", true)), document = DocumentCreation.Insert).isSuccess)

    (for {
      result <- coll.lookupIn(docId, Array(get("foo")))
      content <- result.contentAs[Boolean](0)
    } yield content) match {
      case Success(content: Boolean) =>
        assert(content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def write_and_read_primitive_int() {
    val docId = TestUtils.docId()
    assert(coll.mutateIn(docId, Array(upsert("foo", 42)), document = DocumentCreation.Insert).isSuccess)

    (for {
      result <- coll.lookupIn(docId, Array(get("foo")))
      content <- result.contentAs[Int](0)
    } yield content) match {
      case Success(content) => assert(content == 42)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def write_and_read_primitive_double() {
    val docId = TestUtils.docId()
    assert(coll.mutateIn(docId, Array(upsert("foo", 42.3)), document = DocumentCreation.Insert).isSuccess)

    (for {
      result <- coll.lookupIn(docId, Array(get("foo")))
      content <- result.contentAs[Double](0)
    } yield content) match {
      case Success(content) => assert(content == 42.3)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def write_and_read_primitive_long() {
    val docId = TestUtils.docId()
    assert(coll.mutateIn(docId, Array(upsert("foo", Long.MaxValue)), document = DocumentCreation.Insert)
      .isSuccess)

    (for {
      result <- coll.lookupIn(docId, Array(get("foo")))
      content <- result.contentAs[Long](0)
    } yield content) match {
      case Success(content) => assert(content == Long.MaxValue)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def write_and_read_primitive_short() {
    val docId = TestUtils.docId()
    assert(coll.mutateIn(docId, Array(upsert("foo", Short.MaxValue)), document = DocumentCreation.Insert)
      .isSuccess)

    (for {
      result <- coll.lookupIn(docId, Array(get("foo")))
      content <- result.contentAs[Short](0)
    } yield content) match {
      case Success(content) => assert(content == Short.MaxValue)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }
}
