package com.couchbase.client.scala.search

import com.couchbase.client.scala.Cluster
import org.scalatest.FunSuite

import scala.util.{Failure, Success}

class SearchSpec extends FunSuite {

  // TODO Commenting out for now.  Need a true solution to testing search
//  val cluster = Cluster.connect("localhost", "Administrator", "password")
//  val bucket = cluster.bucket("travel-sample")
//  val coll = bucket.defaultCollection
//
//  test("simple") {
//    cluster.searchQuery(SearchQuery("travel-sample-index-unstored",
//      SearchQuery.queryString("united")).limit(10)) match {
//      case Success(result) =>
//        assert(result.errors.isEmpty)
//        assert(10 == result.allRowsOrErrors.get.size)
//      case Failure(exception) =>
//        fail(exception)
//    }
//  }
}
