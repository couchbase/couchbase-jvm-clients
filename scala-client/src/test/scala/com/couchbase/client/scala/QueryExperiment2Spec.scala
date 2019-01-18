package com.couchbase.client.scala

//import com.couchbase.client.scala.query.{N1qlQueryResult, N1qlRow}
//import org.scalatest.FunSuite
//
//case class N1qlQuery(statement: String) {
////  def execute()(implicit c: Cluster): N1qlQueryResult = execute(c)
//  def execute(cluster: Cluster): N1qlQueryResult = ???
//}
//
//object N1qlQuery {
//  // import with import N1qlQuery._
//  implicit def string2query(v: String): N1qlQuery = N1qlQuery(v)
//}
//
//class QueryExperiment2Spec extends FunSuite {
//
//  import N1qlQuery._
//
//  val cluster = CouchbaseCluster.create("localhost")
//  val bucket = cluster.openBucket("default")
//  val scope = bucket.openScope("scope")
//  val coll = scope.openCollection("people")
//
//  test("1") {
//
//    case class Projection(name: String, age: Int)
//
//    val names = s"""SELECT name, age FROM `${coll.name}`""".execute(cluster)
//      .allRows()
//      .filter(row => row.age.getInt > 5)
//      .filter(_.age.getInt > 5)
//      .map(_.contentAs[Projection])
//
//  }
//}
