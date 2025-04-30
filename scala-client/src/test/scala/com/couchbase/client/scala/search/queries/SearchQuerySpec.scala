package com.couchbase.client.scala.search.queries

import com.couchbase.client.scala.json.JsonObject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SearchQuerySpec {
  @Test
  def searchQueryToStringReturnsQueryJson(): Unit = {
    assertEquals(
      JsonObject.create.put("match", "foo"),
      JsonObject.fromJson(SearchQuery.matchQuery("foo").toString)
    )
  }
}
