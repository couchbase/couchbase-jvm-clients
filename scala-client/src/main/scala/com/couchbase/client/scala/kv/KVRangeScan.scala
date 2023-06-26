package com.couchbase.client.scala.kv

import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.kv.{CoreRangeScanItem, CoreScanTerm}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.codec.{JsonDeserializer, Transcoder, TranscoderWithSerializer, TranscoderWithoutSerializer}

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.{Random, Try}

/** A scan term identifies a point to scan from or to scan to.
  *
  * @param term      matches a particular document id pattern, such as "user_".  Since it's represented as an Array[Byte]
  *                  to support maximum() and minimum(), it's easiest to construct a ScanTerm via the methods in the
  *                  companion object.
  * @param exclusive controls whether this term is inclusive or exclusive - defaults to false.
  */
case class ScanTerm(term: String, exclusive: Boolean = false) {
  private[scala] def toCore: CoreScanTerm = {
    new CoreScanTerm(term, exclusive)
  }
}

object ScanTerm {

  /** Creates a ScanTerm including `term`. */
  def inclusive(term: String): ScanTerm = ScanTerm(term)

  /** Creates a ScanTerm excluding `term`. */
  def exclusive(term: String): ScanTerm = ScanTerm(term, exclusive = true)
}

/** Controls what type of scan is performed. */
sealed trait ScanType

object ScanType {

  /** Scans documents, from document `from` to document `to`.
    *
    * If `None` is specified for `from`, it indicates to scan from the start of the collection.
    * If `None` is specified for `to`, it indicates to scan to the start of the collection.
    */
  case class RangeScan(from: Option[ScanTerm], to: Option[ScanTerm])
      extends ScanType

  /** Samples documents randomly from the collection until reaching `limit` documents.
    *
    * @param seed seed for the random number generator that selects the documents.
    *             If not specified, defaults to a random number.
    *             <b>CAVEAT</b>: Specifying the same seed does not guarantee the same documents are selected.
    */
  case class SamplingScan(limit: Long, seed: Long = Random.nextLong()) extends ScanType

  case class PrefixScan(prefix: String) extends ScanType
}

/** Provides control over how a KV range scan is performed.
  */
case class ScanOptions(
    private[scala] val timeout: Duration = Duration.MinusInf,
    private[scala] val parentSpan: Option[RequestSpan] = None,
    private[scala] val retryStrategy: Option[RetryStrategy] = None,
    private[scala] val transcoder: Option[Transcoder] = None,
    private[scala] val idsOnly: Option[Boolean] = None,
    private[scala] val consistentWith: Option[MutationState] = None,
    private[scala] val batchByteLimit: Option[Int] = None,
    private[scala] val batchItemLimit: Option[Int] = None
) {

  /** Changes the timeout setting used for this operation.
    *
    * When the operation will timeout.  This will default to `timeoutConfig().kvScanTimeout()` in the
    * [[com.couchbase.client.scala.env.ClusterEnvironment]].
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def timeout(value: Duration): ScanOptions = {
    copy(timeout = value)
  }

  /** Changes the parent span setting used for this operation.
    *
    * This allows tracing requests through a full distributed system.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def parentSpan(value: RequestSpan): ScanOptions = {
    copy(parentSpan = Some(value))
  }

  /** Changes the parent span setting used for this operation.
    *
    * This allows tracing requests through a full distributed system.
    *
    * This Option-overload is provided as a convenience to help with chaining.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def parentSpan(value: Option[RequestSpan]): ScanOptions = {
    copy(parentSpan = value)
  }

  /** Changes the transcoder used for this operation.
    *
    * The transcoder provides control over how JSON is converted in the returned `ScanResult`.
    *
    * If not specified it will default to to `transcoder()` in the
    * [[com.couchbase.client.scala.env.ClusterEnvironment]].
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def transcoder(value: Transcoder): ScanOptions = {
    copy(transcoder = Some(value))
  }

  /** Provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
    * in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]], which by default is
    * `BestEffortRetryStrategy`; this will automatically retry some operations (e.g. non-mutating ones, or mutating
    * operations that have unambiguously failed before they mutated state) until the chosen timeout.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def retryStrategy(value: RetryStrategy): ScanOptions = {
    copy(retryStrategy = Some(value))
  }

  /** Just returns each document's id - not the CAS, expiry or content.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def idsOnly(value: Boolean): ScanOptions = {
    copy(idsOnly = Some(value))
  }

  /** The KV range scan will wait until this mutation has been consistently applied.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def consistentWith(value: MutationState): ScanOptions = {
    copy(consistentWith = Some(value))
  }

  /** Controls how many bytes are sent from the server to the client on each partition batch.
    *
    * If both this and `batchItemLimit` are set, the lowest wins.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def batchByteLimit(value: Int): ScanOptions = {
    copy(batchByteLimit = Some(value))
  }

  /** Controls how many documents are sent from the server to the client on each partition batch.
    *
    * If both this and `batchByteLimit` are set, the lowest wins.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def batchItemLimit(value: Int): ScanOptions = {
    copy(batchItemLimit = Some(value))
  }
}

/** A KV range scan operation will return a stream of these.
  *
  * @define SupportedTypes this can be of any type for which an implicit
  *                        `com.couchbase.client.scala.codec.JsonDeserializer` can be found: a list
  *                        of types that are supported 'out of the box' is available at
  *                        [[https://docs.couchbase.com/scala-sdk/1.0/howtos/json.html these JSON docs]]
  */
case class ScanResult private (private val internal: CoreRangeScanItem, transcoder: Transcoder) {

  /** The unique identifier of the document. */
  def id: String = internal.key

  /** Whether the scan was initiated with `idsOnly` set.  If so, only the `id` field is present. */
  def idOnly: Boolean = internal.value.length == 0

  /** The document's CAS value at the time of the lookup.
    * Will not be present if the scan was performed with `idsOnly` set. */
  def cas: Option[Long] = internal.cas() match {
    case 0 => None
    case _ => Some(internal.cas())
  }

  /** The document's expiration time, if it was fetched without the `idsOnly` flag set.  If that flag
    * was not set, this will be None.  The time is the point in time when the document expires. */
  def expiryTime: Option[Instant] = Option(internal.expiry())

  /** If the scan was initiated without the `idsOnly` flag set then this will contain the
    * document's expiration value.  Otherwise it will be None.
    *
    * The time is expressed as a duration from the start of 'epoch time' until when the document expires.
    *
    * Also see [[expiryTime]] which also provides the expiration time, but in the form of the point of time at which
    * the document expires.
    */
  def expiry: Option[Duration] = expiryTime.map(i => Duration(i.getEpochSecond, TimeUnit.SECONDS))

  /** Return the content, converted into the application's preferred representation.
    *
    * @tparam T $SupportedTypes
    */
  def contentAs[T](implicit deserializer: JsonDeserializer[T], tag: ClassTag[T]): Try[T] = {
    val bytes = internal.value()
    val flags = internal.flags()
    transcoder match {
      case t: TranscoderWithSerializer    => t.decode(bytes, flags, deserializer)
      case t: TranscoderWithoutSerializer => t.decode(bytes, flags)
    }
  }
}
