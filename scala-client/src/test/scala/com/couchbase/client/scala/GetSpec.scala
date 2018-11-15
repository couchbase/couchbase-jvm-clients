package com.couchbase.client.scala
import org.scalatest._
import org.scalatest.{FlatSpec, FunSpec, Matchers}
import scala.concurrent.ExecutionContext.Implicits.global

class GetSpec extends FlatSpec with Matchers {

  "a basic get" should "succeed" in {
    val cluster = com.couchbase.client.java.CouchbaseCluster.create("localhost")
    cluster.authenticate("Administrator", "password")
    val bucket = cluster.openBucket("default")
    val scope = new Scope(cluster, bucket, "scope")
    val coll = scope.openCollection("people")
    val docOpt = coll.get("casChangesOnReplace_0")

    assert(docOpt.isDefined)
    assert(docOpt.get.cas != 0)
  }
}
