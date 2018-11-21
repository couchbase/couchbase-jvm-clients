package com.couchbase.client.scala
import java.util.UUID

import org.scalatest._
import org.scalatest.{FlatSpec, FunSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global

//class BasicSpec extends FlatSpec with Matchers with BeforeAndAfterAll  {
//  val cluster = com.couchbase.client.java.CouchbaseCluster.create("localhost")
//  cluster.authenticate("Administrator", "password")
//  val bucket = cluster.openBucket("default")
//  val scope = new Scope(cluster, bucket, "scope")
//  val coll = scope.openCollection("people")
//
//  override def beforeAll() {
//    bucket.bucketManager().flush()
//  }
//
//  "a basic blocking remove" should "succeed" in {
//    val id = docId(0)
//    val doc = JsonObject.create().put("value", "INSERTED")
//    val newDoc = coll.insert(id, doc)
//
//    val result = coll.remove(id, 0)
//
//    assert(result.cas != newDoc.cas)
//    assert(result.cas != 0)
//    assert(result.mutationToken.isEmpty)
//    assert(coll.get(id).isEmpty)
//  }
//
//  "a basic blocking insert and get" should "succeed" in {
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
//
//  def docId(idx: Int): String = {
//    UUID.randomUUID().toString + "_" + idx
//  }
//}
