package com.couchbase.client.scala.query.conversational

import com.couchbase.client.scala.codec.JsonDeserializer
import com.couchbase.client.scala.query.{QueryMetrics, QueryStatus, QueryWarning}

import scala.concurrent.duration.Duration
import scala.util.{Failure, Try}

/** Additional information returned by the query service for a conversational query request, aside from any rows and
  * errors.
  *
  * @param requestId                     the request identifier string of the request
  * @param clientContextId               the client context id of the request
  * @param status                        the status returned from the query service
  * @param warnings                      any warnings returned from the query service
  * @param metrics                       metrics related to the request, if they were enabled
  * @param requestTokens                 model token usage for this request, if returned
  * @param chatTokens                    cumulative model token usage for the chat, if returned
  * @param naturalLanguageProcessingTime time spent on natural-language processing, if returned
  * @param generatedStatementExecuted    whether the generated statement was executed, if returned.  Requires metrics
  *                                      to be enabled
  */
case class ConversationalQueryMetaData(
    requestId: String,
    clientContextId: String,
    status: QueryStatus,
    warnings: collection.Seq[QueryWarning],
    metrics: Option[QueryMetrics],
    requestTokens: Option[TokenUsage],
    chatTokens: Option[TokenUsage],
    naturalLanguageProcessingTime: Option[Duration],
    generatedStatementExecuted: Option[Boolean],
    private val _profileContent: Option[Array[Byte]]
) {

  /** Return the profile content, converted into the application's preferred representation.
    *
    * Note a profile must first be requested with [[ConversationalQueryOptions.profile]] or
    * [[ConversationalQuery.Chat.AskOptions.profile]].
    *
    * @tparam T any supported type; see [[https://docs.couchbase.com/scala-sdk/current/howtos/json.html these JSON docs]] for a full list
    */
  def profileAs[T](implicit deserializer: JsonDeserializer[T]): Try[T] = {
    _profileContent match {
      case Some(content) => deserializer.deserialize(content)
      case _             => Failure(new IllegalArgumentException("No profile is available"))
    }
  }
}
