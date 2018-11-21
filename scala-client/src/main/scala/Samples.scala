import com.couchbase.client.core.Core
import com.couchbase.client.core.env.CoreEnvironment
import com.couchbase.client.scala._
import com.couchbase.client.scala.document.JsonObject
import com.couchbase.client.scala.query.N1qlResult

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Samples {


  def blockingApi(): Unit = {
    val cluster = CouchbaseCluster.create("localhost")
    val bucket = cluster.openBucket("default")

    // Opening a scope should look like this:
    // val scope = bucket.openScope("scope").
    // but for now we're using a mix of SDK2 & prototype SDK3 and need to bridge the gap here.  So ignore next 2 lines.
//    val core = Core.create(CoreEnvironment.create(), null)
//    val scope = new Scope(core, cluster, bucket, "scope")
    val scope = bucket.openScope("scope")

    val coll = scope.openCollection("people")
    // Also supported: val coll = bucket.openCollection("scope", "people")

    // As the methods below block on a Scala Future, they need an implicit ExecutionContext in scope
    implicit val ec = ExecutionContext.Implicits.global

    // All methods have both a named/default parameters version, and an [X]Options version
    val fetched1 = coll.get("id")
    val fetched3 = coll.get("id", timeout = 1000.milliseconds)
    val fetched5 = coll.get("id", GetOptions().timeout(1000.milliseconds))


    // Gets return Option[JsonDocument].  getOrError is a convenience method that either returns JsonDocument (no Option) or throws DocumentNotFoundException
    val fetched2 = coll.getOrError("id")
    val fetched7 = coll.getOrError("id", GetOptions().timeout(1000.milliseconds))

    // getAndLock and getAndTouch work pretty much the same as get
    val fetched4 = coll.getAndLock("id", 5.seconds)


    // Simple subdoc lookup
    val result: FieldsResult = coll.getFields("id", GetFields()
      .getString("field1")
      .getInt("field2"))

    result.content(0).asInstanceOf[String]
    result.content("field1").asInstanceOf[String]
    result.field1.asInstanceOf[String]


    // Subdoc lookup into a projection class
    case class MyProjection(field1: String, field2: Int)

    val result2 = coll.getFieldsAs[MyProjection]("id", GetFields()
      .get("field1")
      .get("field2"))


    // Fulldoc, converted to an entity class
    case class MyUserEntity(id: String, firstName: String, age: Int)

    val user = coll.getAs[MyUserEntity]("id")


    // Various ways of inserting
    val inserted = coll.insert("id", JsonObject.create())
    coll.insert("id", JsonObject.create(), timeout = 1000.milliseconds, expiration = 10.days, replicateTo = ReplicateTo.ALL, persistTo = PersistTo.MAJORITY)
    coll.insert("id", JsonObject.create(), timeout = 1000.milliseconds, persistTo = PersistTo.MAJORITY)
    coll.insert("id", JsonObject.create(), InsertOptions().timeout(1000.milliseconds).persistTo(PersistTo.MAJORITY))


    // Various ways of replacing
    if (fetched1.isDefined) {
      // JsonDocument will be an immutable Scala case class and it's trivial to copy it with different content:
      // val toReplace = fetched1.get.copy(content = JsonObject.empty())
      val toReplace = fetched1.get
      coll.replace(toReplace.id, JsonObject.create(), toReplace.cas)
      coll.replace(toReplace.id, JsonObject.create(), toReplace.cas, timeout = 1000.milliseconds, persistTo = PersistTo.MAJORITY)
      coll.replace(toReplace.id, JsonObject.create(), toReplace.cas, ReplaceOptions().timeout(1000.milliseconds).persistTo(PersistTo.MAJORITY))
    }

    // Queries
    cluster.query("select * from `beer-sample`")

    cluster.query("select * from `beer-sample` where beer = $name",
      QueryOptions().namedParameter("name", "Some beer"))

    cluster.query("select * from `beer-sample` where beer = ? and producer = ?",
      QueryOptions().positionalParameters("Budweiser", "Andheuser-Busch")
        //        .scanConsistency(AtPlus(consistentWith = List(inserted.mutationToken())))
        .timeout(5.seconds))

    cluster.query("select * from `beer-sample`",
      QueryOptions().scanConsistency(StatementPlus())
        .serverSideTimeout(10.seconds))

    case class BeerProjection(name: String, producer: String)

    val beers: N1qlResult[BeerProjection] = cluster.queryAs[BeerProjection]("select name, producer from `beer-sample`")
  }


// There are two asynchronous APIs.  This one returns Scala Futures (similar to a Java CompletableFuture).  The API
// is basically identical to the synchhronous one above, just returning a Future.  Most of this code is just giving
// basic usage for Scala Futures.
def asyncApi(): Unit = {

  // When using Scala Futures you tell them how to run (thread-pools etc.) with an ExecutionContext (similar to a
  // Java executor), normally provided as an implicit argument (e.g. all Futures below will automatically use this
  // variable, as it's in-scope, marked implicit, and has the correct type).  This basic one is a simple thread-pool.
  implicit val ec = ExecutionContext.Implicits.global

  val cluster = CouchbaseCluster.create("localhost")
  val bucket = cluster.openBucket("default")

  // Opening a scope should look like this:
  // val scope = bucket.openScope("scope").
  // but for now we're using a mix of SDK2 & prototype SDK3 and need to bridge the gap here.  So ignore next 2 lines.
  val core = Core.create(CoreEnvironment.create(), null)
  val scope = new Scope(core, cluster, bucket, "scope")

  val coll = scope.openCollection("people").async()

  // Gets return Future[Option[JsonDocument]].  A basic way to handle a Future's result is this:
  coll.get("id", timeout = 1000.milliseconds) onComplete {
    case Success(doc) =>
      // doc is an Option[JsonDocument]
      if (doc.isDefined) println("Got a doc!")
      else println("No doc :(")

    case Failure(err) =>
      // err is a RuntimeException
      println("Error! " + err)
  }

  // Or block on it (discouraged)
  val getFuture = coll.get("id")
  val doc = Await.result(getFuture, atMost = 5.seconds)

  // Futures are powerful and support things like map and filter.  Many of the operations supported by Project Reactor
  // are possible with Futures (though they're missing backpressure and many of Reactor's more advanced operators)
  // Get-and-replace
  val replace = coll.getOrError("id", timeout = 1000.milliseconds)
    .map(doc => {
      coll.replace(doc.id, JsonObject.empty(), doc.cas, timeout = 1000.milliseconds, persistTo = PersistTo.MAJORITY)
    })

  Await.result(replace, atMost = 5.seconds)

  // Another, maybe tidier way of writing that get-replace
  val replace2 = for {
    doc <- coll.getOrError("id", timeout = 1000.milliseconds)
    doc <- {
      // coll.replace(doc.copy(content = JsonObject.empty()))
      coll.replace(doc.id, JsonObject.create(), doc.cas)
    }
  } yield doc

  Await.result(replace, atMost = 5.seconds)

  // Insert
  coll.insert("id", JsonObject.create(), timeout = 1000.milliseconds, persistTo = PersistTo.MAJORITY) onComplete {
    case Success(doc) =>
    case Failure(err) =>
  }

}


// Finally, this API wraps the reactive library Project Reactor
// The API is basically identical to the blocking one except returning Reactor Mono's.  Most of this code is showing
// normal Reactor usage.
// Disabled for now to keep up with rapid prototyping, but it'll look something like this
//  def reactiveAPI(): Unit = {
//    val cluster = CouchbaseCluster.create("localhost")
//    val bucket = cluster.openBucket("default")
//    val scope = new Scope(cluster, bucket, "scope")
//    val coll = scope.openCollection("people").reactive()
//
//    // As the methods below wrap a Scala Future, they need an implicit ExecutionContext in scope
//    implicit val ec = ExecutionContext.Implicits.global
//
//    // Get
//    coll.get("id", timeout = 1000.milliseconds)
//      .map(doc => {
//        if (doc.isDefined) println("Got doc")
//        else println("No doc :(")
//      })
//      // As normal with Reactive, blocking is discouraged - just for demoing
//      .block()
//
//    // Get-replace
//    coll.getOrError("id", timeout = 1000.milliseconds)
//      .flatMap(doc => {
//        // val newDoc = doc.copy(content = JsonObject.empty())
//        val newDoc = doc
//        coll.replace(newDoc)
//      })
//      // As normal with Reactive, blocking is discouraged - just for demoing
//      .block()
//  }
}