package com.couchbase.client.scala

import com.couchbase.client.core.error.{KeyNotFoundException, LockException}
import com.couchbase.client.scala.env.{ClusterEnvironment, SeedNode}
import com.couchbase.client.scala.implicits.Codec
import com.couchbase.client.scala.json.{JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.query.QueryOptions
import com.couchbase.client.scala.util.{ScalaIntegrationTest, Validate}
import com.couchbase.client.test.{ClusterAwareIntegrationTest, ClusterType, IgnoreWhen}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import scala.concurrent.duration._
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

  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  @Test
  def insert() {
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
  def exists() {
    val docId = TestUtils.docId()
    coll.remove(docId)

    coll.exists(docId) match {
      case Success(result) => assert(!result.exists)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    assert(coll.insert(docId, ujson.Obj()).isSuccess)

    coll.exists(docId) match {
      case Success(result) => assert(result.exists)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  private def cleanupDoc(docIdx: Int = 0): String = {
    val docId = TestUtils.docId(docIdx)
    coll.remove(docId)
    docId
  }

  @Test
  def insert_returns_cas() {
    val docId = cleanupDoc()

    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content) match {
      case Success(result) => assert(result.cas != 0)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  @Test
  def insert_without_expiry() {
    val docId = cleanupDoc()

    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content, expiry = 5.seconds).isSuccess)

    coll.get(docId) match {
      case Success(result) => assert(result.expiry.isEmpty)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  @Test
  def insert_with_expiry() {
    val docId = cleanupDoc()

    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content, expiry = 5.seconds).isSuccess)

    coll.get(docId, withExpiry = true) match {
      case Success(result) => assert(result.expiry.isDefined)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  @Test
  def touch() {
    val docId = cleanupDoc()

    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content, expiry = 5.seconds).isSuccess)

    assert(coll.touch(docId, expiry = 10.seconds).isSuccess)

    coll.get(docId, withExpiry = true) match {
      case Success(result) => assert(result.expiry.isDefined)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  @Test
  def get_and_lock() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content).get

    coll.getAndLock(docId, 30.seconds) match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.cas)
        assert(result.contentAs[ujson.Obj].get == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.getAndLock(docId, 30.seconds) match {
      case Success(result) => assert(false, "should not have been able to relock locked doc")
      case Failure(err: LockException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def unlock() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content).get

    coll.getAndLock(docId, 30.seconds) match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.cas)
        assert(result.contentAs[ujson.Obj].get == content)

        coll.unlock(docId, result.cas) match {
          case Success(_) =>
          case Failure(err) => assert(false, s"unexpected error $err")
        }

      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.getAndLock(docId, 30.seconds) match {
      case Success(result) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  @Test
  def get_and_touch() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content, expiry = 10.seconds).get

    assert (insertResult.cas != 0)

    coll.getAndTouch(docId, 1.second) match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.cas)
        assert(result.contentAs[ujson.Obj].get == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def remove() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    assert(coll.remove(docId).isSuccess)

    coll.get(docId) match {
      case Success(result) => assert(false, s"doc $docId exists and should not")
      case Failure(err: KeyNotFoundException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def upsert_when_doc_does_not_exist() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
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
  def upsert_when_doc_does_exist() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content)

    assert (insertResult.isSuccess)

    val content2 = ujson.Obj("hello" -> "world2")
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
  def replace_when_doc_does_not_exist() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val upsertResult = coll.replace(docId, content)

    upsertResult match {
      case Success(result) => assert(false, s"doc should not exist")
      case Failure(err: KeyNotFoundException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  @Test
  def replace_when_doc_does_exist_with_2() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content)

    assert (insertResult.isSuccess)

    val content2 = ujson.Obj("hello" -> "world2")
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
  def replace_when_doc_does_exist_with_3() {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content)

    assert (insertResult.isSuccess)

    val content2 = ujson.Obj("hello" -> "world2")
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
  def validations() {
    val validations: Try[Any] = for {
      _ <- Validate.notNullOrEmpty("", "id")
    } yield null

    assert(validations.isFailure)
  }

  @Test
  def all(): Unit = {
    val collection = coll

    val json = ujson.Obj("hello" -> "world")

    val result: Try[String] = collection.upsert("document-key", json)
      .flatMap(_ => collection.get("document-key", timeout = 10.seconds))
      .flatMap(_.contentAs[JsonObjectSafe])
      .flatMap(_.str("status"))

    result match {
      case Success(status) => println(s"Couchbase is $status")
      case Failure(err) =>    println("Error: " + err)
    }


    case class Address(line1: String)
    case class User(name: String, age: Int, addresses: Seq[Address])
    object User {
      implicit val codec: Codec[User] = Codec.codec[User]
    }

    val user = User("user1", 21, Seq(Address("address1")))

    coll.upsert("user1", user).get

    val statement = s"""select * from `users` where meta().id like 'user%';"""

    val users: Try[Seq[User]] = cluster.query(statement)
      .flatMap(_.allRowsAs[User])

  }
}
