package com.couchbase.client.scala

import com.couchbase.client.scala.search.SearchQuery
import com.couchbase.client.scala.search.facet.SearchFacet
import com.couchbase.client.scala.search.sort.SearchSort
import org.scalatest.FunSuite

class SearchSpec extends FunSuite {
  val defaultQuery = SearchQuery.booleanField(true)

  test("sort") {
    SearchQuery("index", defaultQuery).sort(SearchSort.byId)
    SearchQuery("index", defaultQuery).sort(SearchSort.byField("test"))
    SearchQuery("index", defaultQuery).sort(SearchSort.byField("test").descending(true))
  }

  test("facets") {
    val query = SearchQuery("index", defaultQuery)
      .addFacet("facet1", SearchFacet.term("field1", 10))
      .addFacet("facet2", SearchFacet.numeric("field2", 10)
        .addRange("range1", Some(10), Some(20))
        .addRange("range2", Some(20), None))
      .addFacet("facet3", SearchFacet.date("field3", 10)
        .addRange("range1", Some("2011-01-01T00:00:00"), Some("2011-12-31T23:59:59")))
  }
}
