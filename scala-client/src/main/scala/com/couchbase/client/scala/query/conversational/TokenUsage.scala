package com.couchbase.client.scala.query.conversational

/** Model token usage, as reported by the query service.
  *
  * Each counter is exactly as returned, and is absent if the query service did not return it.
  *
  * @param promptTokens     the number of prompt tokens
  * @param completionTokens the number of completion tokens
  * @param totalTokens      the total number of tokens
  */
case class TokenUsage(
    promptTokens: Option[Int],
    completionTokens: Option[Int],
    totalTokens: Option[Int]
)
