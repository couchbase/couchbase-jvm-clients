package com.couchbase.client.scala

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.error.{
  CasMismatchException,
  DocumentNotFoundException,
  TimeoutException
}
import com.couchbase.client.core.retry.RetryReason
import com.couchbase.client.core.retry.reactor.Retry
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.implicits.Codec
import com.couchbase.client.scala.json.{JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.kv.{
  GetOptions,
  GetResult,
  InsertOptions,
  MutationResult,
  UpsertOptions
}
import com.couchbase.client.scala.util.{ScalaIntegrationTest, Validate}
import com.couchbase.client.test.{ClusterType, IgnoreWhen}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

@TestInstance(Lifecycle.PER_CLASS)
class KeyValueSpec extends ScalaIntegrationTest {

  private var cluster: Cluster = _
  private var coll: Collection = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()

    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
    bucket.waitUntilReady(Duration(30, TimeUnit.SECONDS))
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  @Test
  def insert(): Unit = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    coll.get(docId) match {
      case Success(result) =>
        result.contentAs[ujson.Obj] match {
          case Success(body) =>
            assert(body("hello").str == "world")
          case Failure(err) => assert(false, s"unexpected error $err")
        }
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  def exists(): Unit = {
    val docId = TestUtils.docId()
    coll.remove(docId)

    coll.exists(docId) match {
      case Success(result) => assert(!result.exists)
      case Failure(err)    => assert(false, s"unexpected error $err")
    }

    assert(coll.insert(docId, ujson.Obj()).isSuccess)

    coll.exists(docId) match {
      case Success(result) => assert(result.exists)
      case Failure(err)    => assert(false, s"unexpected error $err")
    }

    coll.remove(docId)

    coll.exists(docId) match {
      case Success(result) => assert(!result.exists)
      case Failure(err)    => assert(false, s"unexpected error $err")
    }
  }

  private def cleanupDoc(docIdx: Int = 0): String = {
    val docId = TestUtils.docId(docIdx)
    coll.remove(docId)
    docId
  }

  @Test
  def insert_returns_cas(): Unit = {
    val docId = cleanupDoc()

    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content) match {
      case Success(result) => assert(result.cas != 0)
      case Failure(err)    => assert(false, s"unexpected error $err")
    }
  }
  @Test
  def insert_without_expiry(): Unit = {
    val docId = cleanupDoc()

    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content, InsertOptions().expiry(5.seconds)).isSuccess)

    coll.get(docId) match {
      case Success(result) => assert(result.expiry.isEmpty)
      case Failure(err)    => assert(false, s"unexpected error $err")
    }
  }

  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  @Test
  def insert_with_expiry(): Unit = {
    val docId = cleanupDoc()

    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content, InsertOptions().expiry(5.seconds)).isSuccess)

    coll.get(docId, GetOptions().withExpiry(true)) match {
      case Success(result) => assert(result.expiry.isDefined)
      case Failure(err)    => assert(false, s"unexpected error $err")
    }
  }

  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  @Test
  def touch(): Unit = {
    val docId = cleanupDoc()

    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content, InsertOptions().expiry(5.seconds)).isSuccess)

    assert(coll.touch(docId, expiry = 10.seconds).isSuccess)

    coll.get(docId, GetOptions().withExpiry(true)) match {
      case Success(result) => assert(result.expiry.isDefined)
      case Failure(err)    => assert(false, s"unexpected error $err")
    }
  }

  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  @Test
  def get_and_lock(): Unit = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content      = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content).get

    coll.getAndLock(docId, 30.seconds) match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.cas)
        assert(result.contentAs[ujson.Obj].get == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.getAndLock(docId, 30.seconds, timeout = 100.milliseconds) match {
      case Success(result) => assert(false, "should not have been able to relock locked doc")
      case Failure(err: TimeoutException) =>
        assert(err.context().requestContext().retryReasons().size() == 1)
        assert(
          err.context().requestContext().retryReasons().iterator().next() == RetryReason.KV_LOCKED
        )
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def unlock(): Unit = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content      = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content).get

    coll.getAndLock(docId, 30.seconds) match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.cas)
        assert(result.contentAs[ujson.Obj].get == content)

        coll.unlock(docId, result.cas) match {
          case Success(_)   =>
          case Failure(err) => assert(false, s"unexpected error $err")
        }

      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.getAndLock(docId, 30.seconds) match {
      case Success(result) =>
      case Failure(err)    => assert(false, s"unexpected error $err")
    }
  }
  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  @Test
  def get_and_touch(): Unit = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content      = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content, InsertOptions().expiry(10.seconds)).get

    assert(insertResult.cas != 0)

    coll.getAndTouch(docId, 1.second) match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.cas)
        assert(result.contentAs[ujson.Obj].get == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def remove(): Unit = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    assert(coll.remove(docId).isSuccess)

    coll.get(docId) match {
      case Success(result)                         => assert(false, s"doc $docId exists and should not")
      case Failure(err: DocumentNotFoundException) =>
      case Failure(err)                            => assert(false, s"unexpected error $err")
    }
  }
  @Test
  def upsert_when_doc_does_not_exist(): Unit = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content      = ujson.Obj("hello" -> "world")
    val upsertResult = coll.upsert(docId, content)

    upsertResult match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.mutationToken.isDefined)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.get(docId) match {
      case Success(result) =>
        assert(result.cas == upsertResult.get.cas)
        assert(result.contentAs[ujson.Obj].get == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }
  @Test
  def upsert_when_doc_does_exist(): Unit = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content      = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content)

    assert(insertResult.isSuccess)

    val content2     = ujson.Obj("hello" -> "world2")
    val upsertResult = coll.upsert(docId, content2)

    upsertResult match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.get.cas)
        assert(result.mutationToken.isDefined)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.get(docId) match {
      case Success(result) =>
        assert(result.cas == upsertResult.get.cas)
        assert(result.contentAs[ujson.Obj].get == content2)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }
  @Test
  def replace_when_doc_does_not_exist(): Unit = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content      = ujson.Obj("hello" -> "world")
    val upsertResult = coll.replace(docId, content)

    upsertResult match {
      case Success(result)                         => assert(false, s"doc should not exist")
      case Failure(err: DocumentNotFoundException) =>
      case Failure(err)                            => assert(false, s"unexpected error $err")
    }
  }
  @Test
  def replace_when_doc_does_exist_with_2(): Unit = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content      = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content)

    assert(insertResult.isSuccess)

    val content2      = ujson.Obj("hello" -> "world2")
    val replaceResult = coll.replace(docId, content2)

    replaceResult match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.get.cas)
        assert(result.mutationToken.isDefined)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.get(docId) match {
      case Success(result) =>
        assert(result.cas == replaceResult.get.cas)
        assert(result.contentAs[ujson.Obj].get == content2)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }
  @Test
  def replace_when_doc_does_exist_with_3(): Unit = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content      = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content)

    assert(insertResult.isSuccess)

    val content2      = ujson.Obj("hello" -> "world2")
    val replaceResult = coll.replace(docId, content2, insertResult.get.cas)

    replaceResult match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.get.cas)
        assert(result.mutationToken.isDefined)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.get(docId) match {
      case Success(result) =>
        assert(result.cas == replaceResult.get.cas)
        assert(result.contentAs[ujson.Obj].get == content2)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def validations(): Unit = {
    val validations: Try[Any] = for {
      _ <- Validate.notNullOrEmpty("", "id")
    } yield null

    assert(validations.isFailure)
  }

  case class Address(line1: String)
  // Need define case class & object here - not in `all` method
  // or else, scala 2.11 will not compile, with error:
  // User is already defined as (compiler-generated) case class companion object User
  case class User(name: String, age: Int, addresses: Seq[Address])
  object User {
    implicit val codec: Codec[User] = Codec.codec[User]
  }

  @Test
  def all(): Unit = {
    val collection = coll

    val json = ujson.Obj("hello" -> "world")

    val result: Try[String] = collection
      .upsert("document-key", json)
      .flatMap(_ => collection.get("document-key", timeout = 10.seconds))
      .flatMap(_.contentAs[JsonObjectSafe])
      .flatMap(_.str("status"))

    result match {
      case Success(status) => println(s"Couchbase is $status")
      case Failure(err)    => println("Error: " + err)
    }

    val user = User("user1", 21, Seq(Address("address1")))

    coll.upsert("user1", user).get

    val statement = s"""select * from `users` where meta().id like 'user%';"""

    val users: Try[scala.collection.Seq[User]] = cluster
      .query(statement)
      .flatMap(_.rowsAs[User])

  }

  case class LargeDocTest(values: Map[String, String])

  object LargeDocTest {
    implicit val codec: Codec[LargeDocTest] = Codec.codec[LargeDocTest]
  }

  @Test
  def convertLargeObject(): Unit = {
    val count   = 100000
    val content = collection.mutable.Map.empty[String, String]
    for (x <- Range(0, count)) {
      content.put(x.toString, "a")
    }
    val id  = TestUtils.docId()
    val obj = LargeDocTest(content.toMap)
    coll.upsert(id, obj).get
    val result = coll.get(id).get
    val as     = result.contentAs[LargeDocTest].get
    assert(as.values.size == count)
  }

  case class LargeDocTest2(values: Set[Int])

  object LargeDocTest2 {
    implicit val codec: Codec[LargeDocTest2] = Codec.codec[LargeDocTest2]
  }

  @Test
  def convertLargeSet(): Unit = {
    val count   = 100000
    val content = collection.mutable.Set.empty[Int]
    for (x <- Range(0, count)) {
      content.add(x)
    }
    val obj = LargeDocTest2(content.toSet)
    val id  = TestUtils.docId()
    coll.upsert(id, obj).get
    val result = coll.get(id).get
    val as     = result.contentAs[LargeDocTest2].get
    assert(as.values.size == count)
  }
}

