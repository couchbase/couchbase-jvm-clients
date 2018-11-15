package com.couchbase.client.scala

import com.couchbase.client.core.message.kv._
import com.couchbase.client.core.message.{CouchbaseRequest, CouchbaseResponse, ResponseStatus}
import com.couchbase.client.core.{CoreContext, CouchbaseCore, CouchbaseException}
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper
import com.couchbase.client.java.bucket.api.Utils.addDetails
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{Document, JsonDocument}
import com.couchbase.client.java.env.CouchbaseEnvironment
import com.couchbase.client.java.error._
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

  def insert(id: String,
             content: JsonObject,
             timeout: FiniteDuration = kvTimeout,
             expiration: FiniteDuration = 0.seconds,
             replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
             persistTo: PersistTo.Value = PersistTo.NONE
            )(implicit ec: ExecutionContext): Future[JsonDocument] = {
    val doc = JsonDocument.create(id, content)
    val encoded = JSON_OBJECT_TRANSCODER.encode(doc)
    val request = new InsertRequest(doc.id(), encoded.value1(), collection.scope.bucket.name())

    dispatch[InsertRequest, InsertResponse](request)
      .map(response => {
        if (response.status().isSuccess) {
          val out = JSON_OBJECT_TRANSCODER.newDocument(doc.id(), doc.expiry(), doc.content(), response.cas(), response.mutationToken())
          out
        }
        // TODO move this to core
        else response.status() match {
          case ResponseStatus.TOO_BIG =>
            throw addDetails(new RequestTooBigException, response)
          case ResponseStatus.EXISTS =>
            throw addDetails(new DocumentAlreadyExistsException, response)
          case ResponseStatus.TEMPORARY_FAILURE | ResponseStatus.SERVER_BUSY =>
            throw addDetails(new TemporaryFailureException, response)
          case ResponseStatus.OUT_OF_MEMORY =>
            throw addDetails(new CouchbaseOutOfMemoryException, response)
          case _ =>
            throw addDetails(new CouchbaseException(response.status.toString), response)
        }
      })
  }

  def insert(id: String,
             content: JsonObject,
             options: InsertOptions
            )(implicit ec: ExecutionContext): Future[JsonDocument] = {
    insert(id, content, options.timeout, options.expiration, options.replicateTo, options.persistTo)
  }

  def replace(id: String,
              content: JsonObject,
              cas: Long,
              timeout: FiniteDuration = kvTimeout,
              expiration: FiniteDuration = 0.seconds,
              replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
              persistTo: PersistTo.Value = PersistTo.NONE
             )(implicit ec: ExecutionContext): Future[JsonDocument] = null

  def replace(id: String,
              content: JsonObject,
              cas: Long,
              options: ReplaceOptions
             )(implicit ec: ExecutionContext): Future[JsonDocument] = {
    replace(id, content, cas, options.timeout, options.expiration, options.replicateTo, options.persistTo)
  }

  def remove(id: String,
             cas: Long,
             timeout: FiniteDuration = kvTimeout,
             replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
             persistTo: PersistTo.Value = PersistTo.NONE
            )(implicit ec: ExecutionContext): Future[RemoveResult] = {
    val request = new RemoveRequest(id, cas, collection.scope.bucket.name())

    dispatch[RemoveRequest, RemoveResponse](request)
      .map(response => {
        if (response.status().isSuccess) {
          val out = RemoveResult(response.cas(), Option(response.mutationToken()))
          out
        }
          // TODO move this to core
        else response.status() match {
          case ResponseStatus.NOT_EXISTS =>
            throw addDetails(new DocumentDoesNotExistException, response)
          case ResponseStatus.EXISTS | ResponseStatus.LOCKED =>
            throw addDetails(new CASMismatchException, response)
          case ResponseStatus.TEMPORARY_FAILURE | ResponseStatus.SERVER_BUSY =>
            throw addDetails(new TemporaryFailureException, response)
          case ResponseStatus.OUT_OF_MEMORY =>
            throw addDetails(new CouchbaseOutOfMemoryException, response)
          case _ =>
            throw addDetails(new CouchbaseException(response.status.toString), response)
        }
      })

  }

  def remove(id: String,
             cas: Long,
             options: RemoveOptions
            )(implicit ec: ExecutionContext): Future[RemoveResult] = {
    remove(id, cas, options.timeout, options.replicateTo, options.persistTo)
  }

  def get(id: String,
          timeout: FiniteDuration = kvTimeout)
         (implicit ec: ExecutionContext): Future[Option[JsonDocument]] = {
    val request = new GetRequest(id, collection.scope.bucket.name())

    dispatch[GetRequest, GetResponse](request)
      .map(response => {
        if (response.status().isSuccess) {
          val doc = JSON_OBJECT_TRANSCODER.decode(id, response.content(), response.cas(), 0, response.flags(), response.status())
          Option(doc)
        }
        // TODO move this to core
        else response.status match {
          case ResponseStatus.NOT_EXISTS =>
            Option.empty[JsonDocument]
          case ResponseStatus.TEMPORARY_FAILURE | ResponseStatus.SERVER_BUSY | ResponseStatus.LOCKED =>
            throw addDetails(new TemporaryFailureException, response)
          case ResponseStatus.OUT_OF_MEMORY =>
            throw addDetails(new CouchbaseOutOfMemoryException, response)
          case _ =>
            throw addDetails(new CouchbaseException(response.status.toString), response)
        }
      })
  }

  def get(id: String,
          options: GetOptions
         )(implicit ec: ExecutionContext): Future[Option[JsonDocument]] = {
    get(id, options.timeout)
  }

  def getOrError(id: String,
                 timeout: FiniteDuration = kvTimeout)
                (implicit ec: ExecutionContext): Future[JsonDocument] = {
    get(id, timeout).map(doc => {
      if (doc.isEmpty) throw new DocumentDoesNotExistException()
      else doc.get
    })
  }

  def getOrError(id: String,
                 options: GetOptions)
                (implicit ec: ExecutionContext): Future[JsonDocument] = {
    getOrError(id, options.timeout)
  }

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
