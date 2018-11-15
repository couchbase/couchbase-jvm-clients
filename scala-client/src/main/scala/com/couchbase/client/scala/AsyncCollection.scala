package com.couchbase.client.scala

import com.couchbase.client.core.message.kv.{GetRequest, GetResponse}
import com.couchbase.client.core.message.{CouchbaseRequest, CouchbaseResponse}
import com.couchbase.client.core.{CoreContext, CouchbaseCore}
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{Document, JsonDocument}
import com.couchbase.client.java.env.CouchbaseEnvironment
import com.couchbase.client.java.transcoder._
import com.couchbase.client.java.transcoder.subdoc.JacksonFragmentTranscoder
import reactor.core.scala.publisher.Mono
import rx.RxReactiveStreams

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future, JavaConversions}
import scala.concurrent.duration.{FiniteDuration, _}




class AsyncCollection(val collection: Collection) {
  private val core = collection.scope.cluster.core()
  private val mapper = new ObjectMapper()
  private val transcoder = new JacksonFragmentTranscoder(mapper)
  private var coreContext: CoreContext = null
  private val kvTimeout = collection.kvTimeout
  private val JSON_OBJECT_TRANSCODER = new JsonTranscoder
  private val JSON_ARRAY_TRANSCODER = new JsonArrayTranscoder
  private val JSON_BOOLEAN_TRANSCODER = new JsonBooleanTranscoder
  private val JSON_DOUBLE_TRANSCODER = new JsonDoubleTranscoder
  private val JSON_LONG_TRANSCODER = new JsonLongTranscoder
  private val JSON_STRING_TRANSCODER = new JsonStringTranscoder
  private val RAW_JSON_TRANSCODER = new RawJsonTranscoder
  private val BYTE_ARRAY_TRANSCODER = new ByteArrayTranscoder
  private val transcoders = Map[Class[_ <: Document[_]], Transcoder[_ <: Document[_], _]](
    JSON_OBJECT_TRANSCODER.documentType() -> JSON_OBJECT_TRANSCODER,
    JSON_ARRAY_TRANSCODER.documentType() -> JSON_ARRAY_TRANSCODER,
    JSON_BOOLEAN_TRANSCODER.documentType() -> JSON_BOOLEAN_TRANSCODER,
    JSON_DOUBLE_TRANSCODER.documentType() -> JSON_DOUBLE_TRANSCODER,
    JSON_LONG_TRANSCODER.documentType() -> JSON_LONG_TRANSCODER,
    JSON_STRING_TRANSCODER.documentType() -> JSON_STRING_TRANSCODER,
    RAW_JSON_TRANSCODER.documentType() -> RAW_JSON_TRANSCODER,
    BYTE_ARRAY_TRANSCODER.documentType() -> BYTE_ARRAY_TRANSCODER
  )

  // All methods are placeholders returning null for now
  def insert(doc: JsonDocument,
             timeout: FiniteDuration = kvTimeout,
             expiration: FiniteDuration = 0.seconds,
             replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
             persistTo: PersistTo.Value = PersistTo.NONE
            )(implicit ec: ExecutionContext): Future[JsonDocument] = null

  def insert(doc: JsonDocument,
             options: InsertOptions
            )(implicit ec: ExecutionContext): Future[JsonDocument] = null

  def insertContent(id: String,
             content: JsonObject,
             timeout: FiniteDuration = kvTimeout,
             expiration: FiniteDuration = 0.seconds,
             replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
             persistTo: PersistTo.Value = PersistTo.NONE
            )(implicit ec: ExecutionContext): Future[JsonDocument] = null

  def replace(doc: JsonDocument,
              timeout: FiniteDuration = kvTimeout,
              expiration: FiniteDuration = 0.seconds,
              replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
              persistTo: PersistTo.Value = PersistTo.NONE
             )(implicit ec: ExecutionContext): Future[JsonDocument] = null

  def replace(doc: JsonDocument,
              options: ReplaceOptions
             )(implicit ec: ExecutionContext): Future[JsonDocument] = null

  def get(id: String,
          timeout: FiniteDuration = kvTimeout)
         (implicit ec: ExecutionContext): Future[Option[JsonDocument]] = {
    val request = new GetRequest(id, collection.scope.bucket.name())

    dispatch[GetRequest, GetResponse](request)
      .map(response => {
        val doc = JSON_OBJECT_TRANSCODER.decode(id, response.content(), response.cas(), 0, response.flags(), response.status())
        Option(doc)
      })
  }

  def get(id: String,
          options: GetOptions
         )(implicit ec: ExecutionContext): Future[Option[JsonDocument]] = Future { Option.empty }

  def getOrError(id: String,
                 timeout: FiniteDuration = kvTimeout): Future[JsonDocument] = null

  def getOrError(id: String,
                 options: GetOptions): Future[JsonDocument] = null

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 timeout: FiniteDuration = kvTimeout)
                (implicit ec: ExecutionContext): Future[Option[JsonDocument]] = Future { Option.empty }

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 options: GetAndLockOptions)
                (implicit ec: ExecutionContext): Future[Option[JsonDocument]] = Future { Option.empty }

  private def dispatch[REQ <: CouchbaseRequest, RESP <: CouchbaseResponse](request: REQ)
                                                                          (implicit ec: ExecutionContext): Future[RESP] = {
    val observable = core.send[RESP](request)
    val reactor = Mono.from(RxReactiveStreams.toPublisher(observable))
    Future {
      // Purely for prototyping until the new sdk3 core is available
      reactor.block()
    }
  }

}
