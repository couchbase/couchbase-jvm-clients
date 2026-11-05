package com.couchbase.client.scala.query.conversational

import com.couchbase.client.core.api.query.CoreModelOptions
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode
import com.couchbase.client.scala.transformers.JacksonTransformers

import scala.jdk.CollectionConverters._

/** Configures the model the query service uses for natural-language processing.
  *
  * When supplied to an operation, it replaces the default model configuration entirely, including `raw`.  Individual
  * fields are not merged.
  *
  * When the effective credential is [[ModelCredential.Capella]], only `provider` and `name` are supported, and
  * supplying any other option will fail the operation.
  */
case class ModelOptions(
    private[scala] val provider: Option[String] = None,
    private[scala] val name: Option[String] = None,
    private[scala] val endpoint: Option[String] = None,
    private[scala] val region: Option[String] = None,
    private[scala] val outputTokenLimit: Option[Int] = None,
    private[scala] val moderation: Option[Boolean] = None,
    private[scala] val raw: Option[Map[String, Any]] = None
) {

  /** Sets the model provider.  This is sent as supplied and not validated by the SDK.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def provider(value: String): ModelOptions = copy(provider = Some(value))

  /** Sets the model name.  This is sent as supplied and not validated by the SDK.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def name(value: String): ModelOptions = copy(name = Some(value))

  /** Sets a provider-specific endpoint URL.  An explicit endpoint can be used without a model credential.
    *
    * Not supported with [[ModelCredential.Capella]].
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def endpoint(value: String): ModelOptions = copy(endpoint = Some(value))

  /** Sets the provider region.
    *
    * Not supported with [[ModelCredential.Capella]].
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def region(value: String): ModelOptions = copy(region = Some(value))

  /** Sets the maximum number of tokens the model may output.
    *
    * Not supported with [[ModelCredential.Capella]].
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def outputTokenLimit(value: Int): ModelOptions = copy(outputTokenLimit = Some(value))

  /** Sets whether provider moderation is enabled.
    *
    * Not supported with [[ModelCredential.Capella]].
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def moderation(value: Boolean): ModelOptions = copy(moderation = Some(value))

  /** Allows providing additional model configuration, for advanced usage and forward-compatibility.
    *
    * Values may be any JSON value.  If an entry conflicts with a typed option set on this object, the typed option
    * takes precedence.
    *
    * Credentials must be supplied with [[ModelCredential]]: `cred_id` and `api_key` entries (compared
    * case-insensitively) will fail the operation.
    *
    * Not supported with [[ModelCredential.Capella]].
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def raw(value: Map[String, Any]): ModelOptions = copy(raw = Some(value))

  // raw may contain secrets.
  override def toString: String =
    s"ModelOptions($provider,$name,$endpoint,$region,$outputTokenLimit,$moderation,${raw.map(_ =>
        "<redacted>"
      )})"

  private[scala] def toCore: CoreModelOptions =
    new CoreModelOptions(
      provider.orNull,
      name.orNull,
      endpoint.orNull,
      region.orNull,
      outputTokenLimit.map(Int.box).orNull,
      moderation.map(Boolean.box).orNull,
      raw.map(v => JacksonTransformers.MAPPER.convertValue(v.asJava, classOf[ObjectNode])).orNull
    )
}
