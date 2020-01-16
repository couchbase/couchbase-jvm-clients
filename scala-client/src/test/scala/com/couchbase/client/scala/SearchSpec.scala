/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.scala

import com.couchbase.client.core.io.netty.search.SearchMock
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.facet.SearchFacet
import com.couchbase.client.scala.search.facet.SearchFacet.{DateRange, NumericRange}
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.search.sort.SearchSort
import org.junit.jupiter.api.Test

class SearchSpec {
  val defaultQuery: SearchQuery = SearchQuery.booleanField(true)

  @Test
  def sort(): Unit = {
    SearchOptions().sort(Seq(SearchSort.IdSort()))
    SearchOptions().sort(Seq(SearchSort.FieldSort("test")))
    SearchOptions().sort(Seq(SearchSort.FieldSort("test", descending = Some(true))))
  }

  @Test
  def facets(): Unit = {
    val facets = Map(
      "facet1" -> SearchFacet.TermFacet("field1", Some(10)),
      "facet2" -> SearchFacet.NumericRangeFacet(
        "field2",
        Seq(NumericRange("range1", Some(10), Some(20)), NumericRange("range2", Some(20), None)),
        Some(10)
      ),
      "facet3" -> SearchFacet.DateRangeFacet(
        "field3",
        Seq(DateRange("range1", Some("2011-01-01T00:00:00"), Some("2011-12-31T23:59:59"))),
        Some(10)
      )
    )
  }

  @Test
  def alltimeouts(): Unit = {
    val json   = getClass.getClassLoader.getResourceAsStream("sdk-testcases/search/alltimeouts.json")
    val result = SearchMock.loadSearchTestCase(json)

    assert(0 == result.rows.size)
    assert(6 == result.metaData.errors.size)
    assert(6 == result.metaData.metrics.errorPartitionCount)
    assert(6 == result.metaData.metrics.totalPartitionCount)
    assert(0 == result.metaData.metrics.successPartitionCount)
  }
}
