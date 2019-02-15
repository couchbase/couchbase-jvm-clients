package com.couchbase.client.scala

//import com.couchbase.client.scala.N1qlQuery._
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Success


/**

  {
    "id": "bob",
    "type": "user",
    "name": "Bob Smith",
    "age": 26,
    "addresses": [
      {
        "address": "123 Fake Street",
        "zipcode": 99501
        "state": "AK"
      }
    ]
  }

*/

//class DemoBackupSpec extends FunSuite {
//  test("demo") {
//    // 1. Collections support
//    // 2. Simplified API
//    // 3. Flexible JSON handling
//    // 4. Easier-to-use subdoc
//    // 5. New durability options
//    // 6. N1QL
//    // 7. Async flavours
//
//    val cluster = CouchbaseCluster.create("localhost")
//    val bucket = cluster.openBucket("bucket")
//    val scope = bucket.openScope("scope")
//    val coll = scope.openCollection("coll")
//
//
//
//    for {
//      rows <- """select * from `collection` where age > 25""".execute(cluster).allRows()
//      x <- rows.
//    }
//
//    val users = """select * from `collection` where age > 25""".execute(cluster).allRows()
//      .map(_.contentAs[User])
//
//
//    case class User(name: String, age: Int)
//    case class Address(address: String, zipcode: Int) {
//      def isAlaskan: Boolean = zipcode >= 99501 && zipcode <= 99524
//    }
//
//    coll.get("id", GetSpec().withExpiry, timeout = 10.seconds) match {
//      case Some(doc) =>
//        val content = doc.contentAsObject
//        content.get("name").asInstanceOf[String]
//        content.str("name")
//        content.name.str
//        assert(content.addresses(0).address.str == "123 Fake Street")
//
//        assert(doc.expiry.isDefined)
//
//        doc.content("type") match {
//          case JsonString("user") =>
//            val user = doc.contentAs[User]
//            assert(user.name == "Bob Smith")
//
//            val newUser = user.copy(name = "Greg")
//
//            coll.replace("bob", newUser, doc.cas)
//
//            val addresses = doc.contentAs[List[Address]]("addresses")
//        }
//
//
//      case class User(name: String, age: Int)
//
//        val user = doc.contentAs[User]
//
//
//      case _ => println("errr!")
//    }
//
//    coll.get("bob", GetSpec().getMany("name", "age", "addresses"), timeout = 10.seconds) match {
//      case Some(doc) =>
//        val name = doc.content("name") match {
//          case JsonString(s) => s
//        }
//        doc.name.str
//        doc.contentAs[Int]("path")
//        doc.age.int
//        val x = doc.contentAs[List[Address]]("addresses")
//        val alaskanAddresses = x.filter(_.isAlaskan)
//
//      case _ =>
//    }
//
//    coll.async().get("bob") onComplete {
//      case Success(Some(doc)) =>
//        val newContent = doc.contentAsObject.put("foo", "bar")
//
//        coll.async().replace("bob", newContent, doc.cas) onComplete {
//          case Success(mr) =>
//          case _ =>
//        }
//      case _ =>
//    }
//
//    val x = for {
//      doc <- coll.async().getOrError("bob")
//      content <- doc.contentAsObject.put("foo", "bar")
//      mr <- coll.async().replace("bob", content, doc.cas)
//      // ...
//    } yield mr
//
//
//    for {
//      doc <- coll.async().getOrError("id")
//      content <- doc.contentAsObject.put("foo", "bar")
//      mr <- coll.replace("id", content, doc.cas)
//    } yield mr
//
//
//
//
//  }
//}
