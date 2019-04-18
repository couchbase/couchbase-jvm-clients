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
import com.couchbase.client.scala.search.SearchQuery
import com.couchbase.client.scala.search.facet.SearchFacet
import com.couchbase.client.scala.search.sort.SearchSort
import org.junit.jupiter.api.Test

class SearchSpec {
  val defaultQuery = SearchQuery.booleanField(true)

  @Test
  def sort() {
    SearchQuery("index", defaultQuery).sort(SearchSort.byId)
    SearchQuery("index", defaultQuery).sort(SearchSort.byField("test"))
    SearchQuery("index", defaultQuery).sort(SearchSort.byField("test").descending(true))
  }

  @Test
  def facets() {
    val query = SearchQuery("index", defaultQuery)
      .addFacet("facet1", SearchFacet.term("field1", 10))
      .addFacet("facet2", SearchFacet.numeric("field2", 10)
        .addRange("range1", Some(10), Some(20))
        .addRange("range2", Some(20), None))
      .addFacet("facet3", SearchFacet.date("field3", 10)
        .addRange("range1", Some("2011-01-01T00:00:00"), Some("2011-12-31T23:59:59")))
  }

  @Test
  def alltimeouts() {
    val json = getClass.getClassLoader.getResourceAsStream("sdk-testcases/search/alltimeouts.json")
    val result = SearchMock.loadSearchTestCase(json)

    assert(6 == result.errors.size)
    assert(6 == result.meta.status.errorCount)
    assert(6 == result.meta.status.totalCount)
    assert(0 == result.meta.status.successCount)
    assert(!result.meta.status.isSuccess)
  }
}
