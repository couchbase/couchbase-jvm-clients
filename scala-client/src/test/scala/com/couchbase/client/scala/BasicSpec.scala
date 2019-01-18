package com.couchbase.client.scala
import java.util.UUID

import com.couchbase.client.scala.document.{JsonObject, JsonType}
import io.circe.Json.JObject
import org.scalatest._
import org.scalatest.{FlatSpec, FunSpec, Matchers}
import io.circe._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}
import ujson._
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import scala.reflect.runtime.universe._

class BasicSpec extends FlatSpec with Matchers with BeforeAndAfterAll  {
  val (cluster, bucket, scope, coll) = (for {
    cluster <- Cluster.connect("localhost", "Administrator", "password")
    bucket <- cluster.bucket("default")
    scope <- bucket.scope("scope")
    coll <- scope.collection("collection")
  } yield (cluster, bucket, scope, coll)) match {
    case Success(result) => result
    case Failure(err) => throw err
  }

  override def beforeAll() {
    // TODO MVP bucketManager
//    bucket.bucketManager().flush()
  }

  "insert and get of ujson" should "succeed" in {
    val id = docId(0)
    val content = ujson.Obj("value" -> "INSERTED")
    assert(coll.insert(id, content).isSuccess)

    coll.get(id) match {
      case Success(result) =>
        val json = result.contentAsUjson
        assert(json("value").str == "INSERTED")
      case Failure(err) => assert(false)
    }
  }

  "circe encode of a case class" should "succeed" in {
    case class User(name: String, age: Int)
    val user = User("graham" ,36)

    val json = user.asJson.toString()
    assert (json.contains("graham"))
  }

  "insert and get using circe" should "succeed" in {
    val id = docId(0)
    case class User(name: String, age: Int)
    val user = User("graham" ,36)
    assert(coll.insert(id, user.asJson).isSuccess)

    coll.get(id) match {
      case Success(result) =>
        val json = decode[User](result.contentAsStr).right.get
        assert(json.name == "graham")
        assert(json.age == 36)
      case Failure(err) => assert(false)
    }
  }

  // TODO check implicit custom decoders work for circe
  // TODO check generic derivation works without macro paradise in app build




  "insert and get of a case class" should "succeed" in {
    val id = docId(0)
    case class User(name: String, age: Int)
    val user = User("graham" ,36)
    assert(coll.insert(id, user).isSuccess)

    coll.get(id) match {
      case Success(result) =>
        val json = result.contentAs[ujson.Obj]
        assert(json("name").str == "graham")
        assert(json("age").num == 36)
      case Failure(err) => assert(false)
    }
  }


  "blocking remove" should "succeed" in {
    val id = docId(0)
//    val doc = JsonObject.create.put("value", "INSERTED")
//    val content = io.circe.JsonObject.fromMap(Map("value" -> "INSERTED"))
    val content = ujson.Obj("value" -> "INSERTED")
    coll.insert(id, content) match {
      case Failure(err) => assert(false)
      case _ =>
    }





//    val result = coll.remove(id, 0)
//
//    assert(result.cas != newDoc.cas)
//    assert(result.cas != 0)
//    assert(result.mutationToken.isEmpty)
//    assert(coll.get(id).isEmpty)
  }

//  "blocking insert and get" should "succeed" in {
//    val id = docId(0)
//    val doc = JsonObject.create().put("value", "INSERTED")
//    val newDoc = coll.insert(id, doc)
//
//    assert(newDoc.id() == id)
//    assert(newDoc.cas() != 0)
//    assert(newDoc.content().getString("value") == "INSERTED")
//
//    val docOpt = coll.get(id)
//
//    assert(docOpt.isDefined)
//    assert(docOpt.get.cas != 0)
//    assert(docOpt.get.content().getString("value") == "INSERTED")
//
//    coll.remove(id, 0)
//  }

  def docId(idx: Int): String = {
    UUID.randomUUID().toString + "_" + idx
  }
}
