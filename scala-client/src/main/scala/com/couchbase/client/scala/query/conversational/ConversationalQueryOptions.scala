package com.couchbase.client.scala.query.conversational

import com.couchbase.client.core.api.query.{
  CoreConversationalQueryOptions,
  CoreConversationalQueryOutput,
  CoreModelCredential,
  CoreModelOptions,
  CoreQueryProfile,
  CoreQueryScanConsistency
}
import com.couchbase.client.core.api.shared.CoreMutationState
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.scala.query.{QueryProfile, QueryScanConsistency}
import com.couchbase.client.scala.transformers.JacksonTransformers
import com.couchbase.client.scala.util.CoreCommonConverters.convert
import com.couchbase.client.scala.util.DurationConversions
import com.couchbase.client.scala.util.DurationConversions._

import java.util.Optional
import java.{lang, time}
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

/** Customize the execution of a one-shot conversational query. */
case class ConversationalQueryOptions(
    private[scala] val model: Option[ModelOptions] = None,
    private[scala] val credential: Option[ModelCredential] = None,
    private[scala] val hint: Option[String] = None,
    private[scala] val execute: Option[Boolean] = None,
    private[scala] val output: Option[ConversationalQueryOutput] = None,
    private[scala] val useKnowledge: Option[Boolean] = None,
    private[scala] val scanConsistency: Option[QueryScanConsistency] = None,
    private[scala] val useReplica: Option[Boolean] = None,
    private[scala] val clientContextId: Option[String] = None,
    private[scala] val metrics: Option[Boolean] = None,
    private[scala] val profile: Option[QueryProfile] = None,
    private[scala] val timeout: Option[Duration] = None,
    private[scala] val rawParameters: Option[Map[String, Any]] = None
) {

  /** Sets the model configuration for this request.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def model(value: ModelOptions): ConversationalQueryOptions = copy(model = Some(value))

  /** Sets the credential the query service uses to authenticate with the model provider.
    *
    * This is separate from Couchbase authentication.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def credential(value: ModelCredential): ConversationalQueryOptions =
    copy(credential = Some(value))

  /** Provides an additional hint to the model.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def hint(value: String): ConversationalQueryOptions = copy(hint = Some(value))

  /** Controls whether the query service executes the generated statement.
    *
    * Note that `execute(true)` does not guarantee execution.  To determine whether the statement was executed,
    * enable [[metrics]] and inspect [[ConversationalQueryMetaData.generatedStatementExecuted]].
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def execute(value: Boolean): ConversationalQueryOptions = copy(execute = Some(value))

  /** Sets the type of statement to generate.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def output(value: ConversationalQueryOutput): ConversationalQueryOptions =
    copy(output = Some(value))

  /** Controls whether the query service uses its configured Knowledge entries.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def useKnowledge(value: Boolean): ConversationalQueryOptions = copy(useKnowledge = Some(value))

  /** Scan consistency for any executed statement.
    *
    * A scan wait can be provided with `QueryScanConsistency.RequestPlus(Some(scanWait))`.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def scanConsistency(value: QueryScanConsistency): ConversationalQueryOptions =
    copy(scanConsistency = Some(value))

  /** Tells the query engine that replicas can be used.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def useReplica(value: Boolean): ConversationalQueryOptions = copy(useReplica = Some(value))

  /** Adds a client context ID to the request, that will be sent back in the response, allowing clients
    * to meaningfully trace requests/responses when many are exchanged.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def clientContextId(value: String): ConversationalQueryOptions =
    copy(clientContextId = Some(value))

  /** Controls whether metrics are returned by the server.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def metrics(value: Boolean): ConversationalQueryOptions = copy(metrics = Some(value))

  /** Set the profiling information level for query execution.  It can be retrieved with
    * [[ConversationalQueryMetaData.profileAs]].
    *
    * This is an Enterprise Edition feature.  On Community Edition the parameter will be accepted, but no profiling
    * information returned.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def profile(value: QueryProfile): ConversationalQueryOptions = copy(profile = Some(value))

  /** Sets a maximum timeout for processing.
    *
    * Note that a timeout reported by the SDK does not guarantee that the query service has stopped processing the
    * request.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def timeout(value: Duration): ConversationalQueryOptions = copy(timeout = Some(value))

  /** Allows providing custom query request parameters, for advanced usage.
    *
    * Parameter names beginning with `natural` (case-insensitively) are reserved and will fail the operation.
    *
    * Note that the values will be passed through a JSON encoder, so do not provide already encoded JSON as the value. If
    * you want to pass objects or arrays, you can use `JsonObject` and `JsonArray` respectively.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def rawParameters(value: Map[String, Any]): ConversationalQueryOptions =
    copy(rawParameters = Some(value))

  private[scala] def toCore: CoreConversationalQueryOptions = {
    val x = this

    val common = CoreCommonOptions.ofOptional(
      timeout.map(v => DurationConversions.scalaDurationToJava(v)).asJava,
      Optional.empty(),
      Optional.empty()
    )

    new CoreConversationalQueryOptions {
      override def model(): CoreModelOptions = x.model.map(_.toCore).orNull

      override def credential(): CoreModelCredential = x.credential.map(convert).orNull

      override def hint(): String = x.hint.orNull

      override def execute(): lang.Boolean = x.execute.map(Boolean.box).orNull

      override def output(): CoreConversationalQueryOutput = x.output.map(convert).orNull

      override def useKnowledge(): lang.Boolean = x.useKnowledge.map(Boolean.box).orNull

      override def scanConsistency(): CoreQueryScanConsistency = x.scanConsistency match {
        case Some(_: QueryScanConsistency.RequestPlus) => CoreQueryScanConsistency.REQUEST_PLUS
        case Some(QueryScanConsistency.NotBounded)     => CoreQueryScanConsistency.NOT_BOUNDED
        case _                                         => null
      }

      override def consistentWith(): CoreMutationState = x.scanConsistency match {
        case Some(QueryScanConsistency.ConsistentWith(cw)) =>
          new CoreMutationState(cw.tokens.asJava)
        case _ => null
      }

      override def scanWait(): time.Duration = x.scanConsistency match {
        case Some(QueryScanConsistency.RequestPlus(Some(scanWait))) => scanWait
        case _                                                      => null
      }

      override def useReplica(): lang.Boolean = x.useReplica.map(Boolean.box).orNull

      override def clientContextId(): String = x.clientContextId.orNull

      override def metrics(): lang.Boolean = x.metrics.map(Boolean.box).orNull

      override def profile(): CoreQueryProfile = x.profile match {
        case Some(QueryProfile.Off)     => CoreQueryProfile.OFF
        case Some(QueryProfile.Phases)  => CoreQueryProfile.PHASES
        case Some(QueryProfile.Timings) => CoreQueryProfile.TIMINGS
        case _                          => null
      }

      override def raw(): JsonNode = x.rawParameters match {
        case Some(value) =>
          JacksonTransformers.MAPPER.convertValue(value.asJava, classOf[ObjectNode])
        case _ => null
      }

      override def commonOptions(): CoreCommonOptions = common
    }
  }
}

object ConversationalQueryOptions {
  val Default = ConversationalQueryOptions()
}
