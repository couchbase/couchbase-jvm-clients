package com.couchbase.client.scala

import java.util.UUID

import org.scalatest.{FlatSpec, Matchers, _}


import scala.util.{Failure, Success, Try}

class JsonExplorationSpec extends FlatSpec with Matchers with BeforeAndAfterAll  {

  private val DocId = "doc"
  private val (cluster, bucket, scope, coll) = (for {
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
    coll.remove(DocId)
  }

  /**
    * Encoding: returns an Array[Byte] and a flags bitmask (JSON, binary, etc.)
    * Decode: takes an Array[Byte], cas, expiry, flags bitmask and returns a T
    */


  /**
    * Scala already has a number of great, popular JSON libraries.  Instead of re-inventing the wheel, the philosophy
    * is to provide direct support for a handful of these, and extend that support based on what our customers request.
    */

  /**
    * upickle is a lightweight fast serialization library that includes a good JSON lib called usjon.
    *
    * upickle includes a JSON AST which is very friendly to use.  Unusually for Scala libs, it's mutable - the author
    * argues that JSON tends to be very short-lived and used in just one scope, so it doesn't gain much from
    * immutability - while mutability makes it much easier to modify JSON.
    *
    * It's also highly performant and has a perhaps unique property of being able to encode/decode directly into Array[Byte],
    * without needed to go through JSON AST first.
    *
    * I really like this lib and may make this the default for all examples.
    */
  "inserting upickle/ujson AST" should "succeed" in {
    val content = ujson.Obj("value" -> "INSERTED")
    assert(coll.insert(DocId, content).isSuccess)

//    // Both coll.get and doc.contentAs return Try.  The nicest way to handle nested Try's I've found so far is a for-comprehension
//    // like this:
//    (for {
//      doc <- coll.get(id)
//      content <- doc.contentAs[ujson.Obj]
//
//    } yield content) match {
//      case Success(result: ujson.Obj) =>
//        assert(result("value").str == "INSERTED")
//      case Failure(err) => assert(false)
//    }
  }

  /**
    * Like most Scala JSON libs, upickle can convert Scala case classes directly to/from JSON.
    *
    * But like almost all JSON libs I've found (except circe, covered below), it requires the app to write a small
    * amount of boilerplate to support it.
    */

  case class Address(address: String)
  object Address {
    // upickle requires adding this implicit thing to support conversion to/from JSON.  It is at least not much
    // hassle to write.
    implicit val rw: upickle.default.ReadWriter[Address] = upickle.default.macroRW
  }

  case class User(name: String, age: Int, address: Seq[Address])
  object User {
    implicit val rw: upickle.default.ReadWriter[User] = upickle.default.macroRW
  }

  // TODO MVP check provided with json libs

  "inserting case class with upickle/ujson" should "succeed" in {
    import upickle.default._
    import ujson.BytesRenderer

    val content = User("John Smith", 29, List(Address("123 Fake Street")))

    // use upickle to do the conversion
    val bytes: Array[Byte] = transform(content).to(BytesRenderer()).toBytes

    assert(coll.insert(DocId, bytes).isSuccess)

    // Both coll.get and doc.contentAs return Try.  The nicest way to handle nested Try's I've found so far is a for-comprehension
    // like this:
//    coll.get(id) match {
//      case Success(doc) =>
//        val bytes: Array[Byte] = doc.contentAsBytes
//
//        // TO
//        val str = bytes.map(_.toChar).mkString.stripPrefix("\"").stripSuffix("\"")
//        val decoded2 = upickle.default.read[User](str)
//        val decoded = upickle.default.read[User](bytes)
//        assert(decoded.name == "John Smith")
//
//      case Failure(err) => assert(false)
//    }
  }

  /**
    * circe is a fast, popular, JSON library that may have become the defacto standard (not completely sure).
    *
    *
    */
  "using circe AST" should "succeed" in {
    val id = docId(0)

    // Using ujson's AST here, which is very easy to use
    val content = ujson.Obj("value" -> "INSERTED")
    content("value") = "CHANGED" // OMG mutable data!

    // Pass ujson's AST directly to Couchbase
    assert(coll.insert(id, content).isSuccess)

    // Both coll.get and doc.contentAs return Try.  The nicest way to handle nested Try's I've found so far is a for-comprehension
    // like this:
    (for {
      doc <- coll.get(id)
      content <- doc.contentAs[ujson.Obj]

    } yield content) match {
      case Success(result: ujson.Obj) => // do something with content
      case Failure(err) => assert(false)
    }
  }

  /**
    * As a fallback, support Array[Byte] directly
    * Here we'll use uJson to encode and decode Array[Byte], but any JSON lib that can support that could be used
    * When given Array[Byte], the lib will assume it's JSON and send the corresponding flag
    */
  "using Array[Byte] directly" should "succeed" in {
    val id = docId(0)

    val content = ujson.Obj("value" -> "INSERTED")
    val encoded: Array[Byte] = ujson.transform(content, ujson.BytesRenderer()).toBytes

    // Pass Array[Byte] directly to Couchbase
    assert(coll.insert(id, encoded).isSuccess)

    coll.get(id) match {
      case Success(doc) =>
        val bytes: Array[Byte] = doc.contentAsBytes

        // Manipulate the returned bytes - here, using upickle to turn them into ujson's AST
        // TODO MVP get working
        val decoded =  upickle.default.read[ujson.Obj](bytes)

      case Failure(err) => assert(false)
    }
  }

  "using upickle/ujson for bytes" should "succeed" in {
    val id = docId(0)

    val content = ujson.Obj("value" -> "INSERTED")
    val encoded: Array[Byte] = ujson.transform(content, ujson.BytesRenderer()).toBytes
    val enc1 = encoded.map(_.toChar).mkString
    val decoded =  upickle.default.read[ujson.Obj](encoded)
    assert(1==1)

    assert(coll.insert(id, encoded).isSuccess)

    coll.get(id) match {
      case Success(doc) =>
        val bytes: Array[Byte] = doc.contentAsBytes

        val enc2 = bytes.map(_.toChar).mkString
        // Manipulate the returned bytes - here, using upickle to turn them into ujson's AST
        // TODO MVP get working
        val decoded =  upickle.default.read[ujson.Obj](bytes)

      case Failure(err) => assert(false)
    }

  }



    def docId(idx: Int): String = {
    UUID.randomUUID().toString + "_" + idx
  }
}
