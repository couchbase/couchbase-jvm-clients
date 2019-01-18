//package com.couchbase.client.scala
//
//import org.scalatest.FunSuite
//import N1qlQuery._
//import com.couchbase.client.core.error.DocumentDoesNotExistException
//import com.couchbase.client.scala.api._
//import com.couchbase.client.scala.document._
//import com.couchbase.client.scala.query.N1qlRow
//import reactor.core.publisher.Mono
//
//import scala.concurrent.Await
//import scala.concurrent.duration._
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.util.{Failure, Success}
//
//
///**
//  {
//    "id": "bob",
//    "type": "user",
//    "name": "Bob Smith",
//    "age": 26,
//    "addresses": [
//      {
//        "address": "123 Fake Street",
//        "zipcode": 99501
//        "state": "AK"
//      }
//    ]
//  }
//  */
//
//class DemoSpec extends FunSuite {
//  test("demo") {
//
//
//    // 1. Add support for Collections - DP in Mad Hatter
//
//    val cluster = Cluster.connect("localhost", "Administrator", "password")
//    val bucket = cluster.bucket("bucket")
//
//    val coll = bucket.scope("scope").collection("users")
//    val coll2 = bucket.defaultCollection()
//
//    // (type inference)
//
//
//    // 2. Simplified API
//
//    // 2.1 Get rid of overloads (an order of magnitude less) with named & default parameters
//    coll.get("id")
//    coll.get("id", timeout = 10.seconds)
//    coll.get("id", timeout = 10.seconds, withExpiry = true)
//    coll.replace("id", JsonObject.create, 0, timeout = 1500.milliseconds)
//    coll.insert("id", JsonObject.create, durability = Durability.Majority)
//    coll.upsert("id", JsonObject.create)
//    coll.mutateIn("id", MutateInSpec().replace("age", 28).remove("addresses"))
//
//
//    // In Java:
//    // coll.replace("id", JsonObject.create(), 0, replaceOptions().timeoutMsecs(1500).replicateTo(ReplicateTo.One));
//
//
//    // 2.2 Merge subdoc and make it easier to use (one method, same result)
//    val doc0: Option[GetResult] = coll.get("bob") // fulldoc
//    val doc1: Option[GetResult] = coll.get("bob", GetSpec().getDoc) // fulldoc
//    val doc2: Option[GetResult] = coll.get("bob", GetSpec().getMany("name", "age")) // subdoc
//    val doc3: Option[GetResult] = coll.get("bob", GetSpec().getDoc.getMany("name", "age")) // subdoc
//
//    // (remains controversial!)
//
//
//    // 2.3 Making expiry clearer - a common customer complaint
//
//    val doc4 = coll.get("id", withExpiry = true).get // does subdoc call under the hood
//    doc4.expiry
//
//    assert (doc4.expiry.isDefined && doc4.expiry.get == 1.day)
//
//
//    // 2.4 Tidying up throughout the API, choosing better abstraction levels
//    val mr: MutationResult = coll.remove("id", cas = 0)
//    // instead of 'JsonDocument doc = bucket.remove("id");' with empty content
//
//
//
//    // 3. Support new durability options of Mad Hatter
//    coll.replace("id", JsonObject.create, cas = 0, durability = Durability.PersistToMajority)
//
//    // Open question - how to best support 'old' and 'new' durability?
//    coll.replace("id", JsonObject.create, cas = 0, replicateTo = ReplicateTo.One, persistTo = PersistTo.None)
//
//
//
//    // 4. Design decisions - FP vs OOP
//    // Scala is mixed paradigm, can use as a better Java or a hardcore FP language,
//    // or anywhere in-between
//    //
//    // Any Scala API decides where on that spectrum it should be
//    //
//    // As a database API, it's unavoidably a bag of messy non-FP side-effects
//    //
//    // Design approach: Practicality, ease-of-use and convenience first... functional second
//    //
//    // Some specifics...
//
//
//    // 4.1 Replace nulls with Option (same in Java)
//    val docOpt: Option[GetResult] = coll.get("id")
//
//    // Check with pattern matching
//    docOpt match {
//      case Some(doc) => doc.content
//      case None => println("argh!")
//    }
//
//
//    // 4.2 doc.content (JSON) is a recursive algebraic data type
//    coll.get("id") match {
//      case Some(doc) =>
//
//        val content: JsonType = doc.content
//
//        content match {
//          case JsonString(value) =>
//          case JsonBoolean(value) =>
//          case JsonNull() =>
//          case JsonNumber(number) =>
//          case JsonObject(content) =>
//          case JsonArray(values) =>
//        }
//
//
//        // Algebraic data types are preferred for functional languages, very easy to recursively handle:
//        def walk(v: JsonType): Unit = {
//          v match {
//            case JsonString(value) => println("string: " + value)
//            case JsonBoolean(value) => println("boolean: " + value)
//            case JsonNull() => println("null")
//            case JsonNumber(number) => println("number: " + number)
//            case x: JsonObject =>
//              println("object: ")
//              x.names.foreach(x.get(_).map(walk))
//            case x: JsonArray =>
//              println("array: ")
//              x.values.foreach(walk)
//          }
//        }
//
//        walk(doc.content)
//
//
//        // Mainly for the power users - lots of convenience overloads too (shown later)
//    }
//
//
//    // 4.3 Data is often mutable
//    coll.get("id") match {
//      case Some(doc) =>
//        val content = doc.contentAsObject
//
//        content.put("age", 27)
//
//        coll.replace(doc.id, content, doc.cas)
//
//        /*
//        Compare with changing one field in a popular Scala JSON library:
//
//            val data2 = data.withObject(o =>
//              JsonObject.fromTraversableOnce(
//                o.fields.map{f =>
//                  f -> (if (f == "age") 27 else o(f).get)
//                }
//              )
//            )
//         */
//    }
//
//
//    // 4.4 Exceptions thrown rather than Either
//    try {
//      val r: MutationResult = coll.remove("id", cas = 0)
//    }
//    catch {
//      case err: DocumentDoesNotExistException => println("error!")
//    }
//
//    /*
//    rather than the more functional
//
//    coll.remove("id", cas = 0) match {
//      case Left(err) => println("err!")
//      case Right(mutationResult) => println("success!")
//    }
//
//    (may change my mind on this!)
//    */
//
//
//
//
//    // 5. Flexible JSON handling
//    coll.get("bob") match {
//      case Some(doc) =>
//
//        // 5.1 Deferred decoding
//        // Simplifies by removing RawJsonDocument & SerializableDocument
//
//
//        // 5.2 Convenient overloads and accessing by path
//        val c1: JsonType = doc.content
//        c1 match {
//          case x: JsonObject =>
//          case _ => println("not expected type")
//        }
//        val c2: JsonObject = doc.contentAsObject
//        val c3: JsonObject = doc.contentAsObject("addresses[0]")
//        val c4: JsonArray = doc.contentAsArray("addresses")
//        val c5 = doc.contentAs[String]("name") // "Bob Smith"
//        val c6 = doc.contentAs[Int]("age") // 26
//
//
//        // 5.3 Dynamic support
//        val name = doc.name.getString    // "Bob Smith"
//        val age = doc.age.getInt   // 26
//        val zipcode = doc.addresses(0).zipcode.getInt  // 99501
//
//
//        // 5.4 Casting into a class (also in Java)
//        // Catching up with .Net & Go
//        case class User(name: String, age: Int, addresses: List[Address])
//        case class Address(address: String, zipcode: Int, state: String)
//        // (case classes are an easy way of creating an immutable value class)
//
//        val user = doc.contentAs[User]
//        val addresses = doc.contentAs[List[Address]]("addresses")
//        val firstAddress = doc.contentAs[Address]("addresses[0]")
//
//
//        // 5.5 Casting based on type field
//        doc.content("type") {
//          case JsonString("user") =>
//            val user = doc.contentAs[User]
//
//          case JsonString("item") =>
//            // ...
//
//        }
//    }
//
//
//
//    /* 5.6 Same flexibility on subdoc.  Lots of casting in Java sdk2:
//
//    DocumentFragment<Lookup> result = bucket.lookupIn("bob").get("name").get("age").execute();
//    String name = (String) result.content(0);
//    int age = (Integer) result.content("age");
//    */
//
//    coll.get("bob", GetSpec().getMany("name", "age")) match {
//      case Some(doc) =>
//        case class Projection(name: String, age: Int)
//
//        val result = doc.contentAs[Projection]
//    }
//
//
//
//    // 6. Async
//    // - Current Java sdk's async option is RxJava, not trivial to learn
//    // - Scala Future allows us to offer 2nd async api that's easier and more native than reactive
//    val async = coll.async()
//
//    val future = async.get("id")
//
//    // Do something on result
//    future onComplete {
//      case Success(Some(doc)) => // do something with doc
//      case Failure(err) => println("failed!")
//    }
//
//    // Block for result (discouraged)
//    val docFuture = Await.result(future, atMost = 10.seconds)
//
//
//    // 6.1 Easy to combine Futures with for-comprehensions
//    for {
//      doc <- async.getOrError("bob")
//      content <- doc.contentAsObject.put("age", 28)
//      mr <- async.replace("bob", content, doc.cas)
//    } yield mr
//
//
//    // 6.2 Still have reactive API but it's now based on Project Reactor (RxJava is EOL) - hasn't changed much
//    val reactive = coll.reactive()
//
//    reactive.getOrError("id")
//      .flatMap(doc => {
//        reactive.remove(doc.id, doc.cas)
//      })
//      .block()
//
//
//    // 7. N1QL improvements
//
//    case class User(name: String, age: Int)
//
//    val rows: List[N1qlRow] = cluster.query("select * from users.regular").allRows()
//
//    // N1qlRow is very similar to GetResult, with same flexibility
//
//    // Cast all rows to User
//    val users: List[User] = rows.map(_.contentAs[User])
//
//    // Get just the states, using Dynamic
//    val states: List[String] = rows.map(_.addresses(0).state.getString)
//
//    // Can leverage the powerful Scala collections abilities, e.g. filtering to users in Alaska and then grouping by zipcode:
//    val groupedByZipcode = rows
//      .filter(_.addresses(0).state.getString == "AK")
//      .groupBy(_.addresses(0).zipcode.getInt)
//
//    val inZipcode99501: List[N1qlRow] = groupedByZipcode(99501)
//
//
//
//    // Questions?
//    // Please ping me!  (Graham Pople)   or #sdk-and-sdkqe
//  }
//}
